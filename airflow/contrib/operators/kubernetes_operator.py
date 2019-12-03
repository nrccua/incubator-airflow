from airflow import configuration
from airflow.models import BaseOperator, TaskInstance
from airflow.version import version as airflow_version
from airflow.contrib.utils.kubernetes_utils import dict_to_env, uniquify_job_name, deuniquify_job_name, \
    namespaced_kubectl, KubernetesSecretParameter
from airflow.contrib.utils.parameters import enumerate_parameters
from datetime import datetime
import ujson as json
import logging
import os
import re
import subprocess
import tempfile
import time
import yaml
from airflow.utils.state import State
from airflow.utils.db import provide_session
from airflow.contrib.utils.kubernetes_utils import retryable_check_output


class KubernetesJobOperator(BaseOperator):
    template_fields = ('service_account_secret_name',)

    def __init__(self,
                 job_name,
                 container_specs,
                 env=None,
                 volumes=None,
                 service_account_secret_name=None,
                 sleep_seconds_between_polling=15,
                 cloudsql_connections=None,
                 die_if_duplicate=False,
                 *args,
                 **kwargs):

        """
        KubernetesJobOperator will:
        1. Create a job given a Kubernetes job yaml
        2. Poll for the job's success/failure
        3. a. If job succeeds,
           b. If pod fails, raise Exception and do not delete.
        4. delete job (and related pods)
            A separate process should be run to clean old dead jobs.

        :param job_name: Name of the Kubernetes job. Will be suffixed at runtime
        :type job_name: string
        :param container_specs: Specification for the containers to launch. Environment variables will be
            added automatically, as well as the volume of the service_account_secret
        :type container_specs: list
        :param env: Optional additional environment variables to provide to each container
        :type env: dictionary
        :param volumes: Optional additional volumes to make available for mounts. See Kubernetes API documentation for
            details. https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.10/#volume-v1-core
        :type volumes: list[dictionary].
        :param service_account_secret_name: Optional secret to use with Google APIs
        :type service_account_secret_name: string
        :param sleep_seconds_between_polling: number of seconds to sleep between polling
            for job completion, defaults to 60
        :type sleep_seconds_between_polling: int
        :param cloudsql_connections: A list of CloudSQLConnection to tell cloudsql_proxy to open additional connections
        :type cloudsql_connections: list[CloudSQLConnection]
        """
        super(KubernetesJobOperator, self).__init__(*args, **kwargs)
        self.job_name = job_name
        self.instance_names = []
        self.service_account_secret_name = service_account_secret_name
        self.die_if_duplicate = die_if_duplicate
        if self.service_account_secret_name == '':
            self.service_account_secret_name = None
        self.container_specs = []
        for cs in container_specs:
            if hasattr(cs, 'to_dict'):
                self.container_specs.append(cs.to_dict())
            else:
                self.container_specs.append(cs)

        # this expression came out of a docker runtime message
        name_validator = re.compile(
            '^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?(?:\.[a-z0-9](?:[-a-z0-9]*[a-z0-9])?)*$')

        if 1 != len(name_validator.findall(self.job_name)):
            raise ValueError(
                "Invalid job_name: %s. Validated with %s" %
                (self.job_name, name_validator.pattern)
            )

        for cs in self.container_specs:
            if 1 != len(name_validator.findall(cs['name'])):
                raise ValueError(
                    "Invalid container name: %s. Validated with %s" %
                    (cs['name'], name_validator.pattern)
                )

        self.env = env or {}
        self.env['AIRFLOW_VERSION'] = airflow_version

        self.sleep_seconds_between_polling = sleep_seconds_between_polling

        # TODO: dangermike (2018-05-22) get this from... somewhere else
        self.cloudsql_instance_creds = 'airflow-cloudsql-instance-credentials'
        self.cloudsql_db_creds = 'airflow-cloudsql-db-credentials'

        self.cloudsql_connections = (cloudsql_connections or [])

        self.volumes = volumes or []

    @staticmethod
    def from_job_yaml(job_yaml_string,
                      service_account_secret_name=None,
                      sleep_seconds_between_polling=60,
                      *args,
                      **kwargs
                      ):
        """
        Make a Kubernetes job operator from YAML

        :param job_yaml_string: Kubernetes job yaml as a formatted string
        :type job_yaml_string: string
        :param service_account_secret_name: Secret to use. If not provided the function
            will look at the secret assigned to the first volume.
        :type service_account_secret_name: string
        :param sleep_seconds_between_polling: number of seconds to sleep between polling
            for job completion, defaults to 60
        :type sleep_seconds_between_polling: int
        :return: KubernetesJobOperator instance
        """
        job_data = yaml.safe_load(job_yaml_string)

        # clean up the name if this is a round-trip/reuse kind of scenario
        job_name = deuniquify_job_name(job_data['metadata']['name'])

        # this is potentially Bluecore specific: secrets are optional
        service_account_secret_name = service_account_secret_name or \
                                      job_data['spec']['template']['spec']['volumes'][0]['secret']['secretName']

        # this is gross and horrible, but we have to convert the list-of-name/value-dicts that
        # are environment variables in a container spec into a dictionary.
        container_specs = []
        for cs in job_data['spec']['template']['spec']['containers']:
            env = {}
            for e in cs['env']:
                env[e['name']] = e['value']
            cs['env'] = env
            container_specs.append(cs)

        volumes = job_data['spec']['template']['spec'].get('volumes', [])

        return KubernetesJobOperator(
            job_name,
            container_specs=container_specs,
            volumes=volumes,
            service_account_secret_name=service_account_secret_name,
            sleep_seconds_between_polling=sleep_seconds_between_polling,
            *args,
            **kwargs
        )

    def clean_up(self, job_name):
        """
        Deletes the job. Deleting the job deletes are related pods.
        """
        logging.info("KILLING job {}".format(str(job_name)))
        try:
            result = retryable_check_output(args=namespaced_kubectl() + [
                'delete',
                '--ignore-not-found=true',  # in case we hit an edge case on retry
                'job',
                job_name
            ])
        except subprocess.CalledProcessError:
            logging.error("Failed to delete pod - it may have already been destroyed.")
        logging.info(result)

    def on_kill(self):
        """
        Run clean up. Fail the task.
        """
        if 0 < len(self.instance_names):
            # should be exactly one, but let's not mess around
            for name in self.instance_names:
                self.clean_up(name)
            raise Exception('Job %s was killed.' % ','.join(self.instance_names))
        else:
            raise Exception('Job was killed')

    def get_pods(self, job_name):
        return json.loads(retryable_check_output(
            args=namespaced_kubectl() + ['get', 'pods', '-o', 'json', '-l', 'job-name==%s' % job_name]))

    def log_container_logs(self, job_name, pod_output=None):
        """
        Reads the logs from each container in each pod in the job, re-logs them back

        :param job_name: job that owns the pods with the containers we want to log
        :param pod_output: Result of get_pods(job_name) call. If None, will be
                           requested. This is a convenience so we can share/
                           reuse the results of get_pods()
        :return:
        """
        pod_output = pod_output or self.get_pods(job_name)
        for pod in pod_output['items']:
            pod_name = pod['metadata']['name']
            for container in pod['spec']['containers']:
                container_name = container['name']
                extra = dict(pod=pod_name, container=container_name)
                logging.info('LOGGING OUTPUT FROM JOB [%s/%s]:' % (pod_name, container_name), extra=extra)
                output = retryable_check_output(args=namespaced_kubectl() + ['logs', pod_name, container_name])
                for line in output.splitlines():
                    logging.info(line, extra=extra)

    def poll_job_completion(self, job_name, dependent_containers={'cloudsql-proxy'}):
        """
        Polls for completion of the created job.
        Sleeps for sleep_seconds_between_polling between polling.
        Any failed pods will raise an error and fail the KubernetesJobOperator task.
        """
        logging.info('Polling for completion of job: %s' % job_name)
        pod_output = None  # keeping this out here so we can reuse it in the "finally" clause

        try:
            has_live_existed = False
            while True:
                time.sleep(self.sleep_seconds_between_polling)

                pod_output = self.get_pods(job_name)

                job_description = json.loads(
                    retryable_check_output(namespaced_kubectl() + ['get', 'job', "-o", "json", job_name])
                )

                status_block = job_description['status']

                if 'succeeded' in status_block and 'failed' in status_block:
                    raise Exception("Invalid status block containing both succeeded and failed: %s",
                                    json.dumps(status_block))

                if 'active' in status_block:
                    status = 'running'
                elif 'failed' in status_block:
                    status = "failed"
                elif 'succeeded' in status_block:
                    status = 'complete'
                else:
                    status = "pending"

                logging.info('Current status is: %s' % status)

                if "pending" == status:
                    pass

                if "failed" == status:
                    self.log_container_logs(job_name)
                    raise Exception('%s has failed pods, failing task.' % job_name)

                if "complete" == status:
                    return pod_output

                # Determine if we have any containers left running in each pod of the job.
                # Dependent containers don't count.
                # If there are no pods left running anything, we are done here. Cleaning up
                # dependent containers will be left to the top-level `finally` block down below.
                has_live = False
                for pod in pod_output['items']:
                    if 'Unknown' == pod['status']['phase']:
                        # we haven't run yet
                        has_live = True
                        break
                    elif 'Pending' == pod['status']['phase']:
                        has_live = True
                        start_time_s = pod['status'].get('startTime')
                        if not start_time_s:
                            logging.info('Pod not yet started')
                            break
                        start_time = datetime.strptime(start_time_s, "%Y-%m-%dT%H:%M:%SZ")
                        start_duration_secs = (datetime.utcnow() - start_time).total_seconds()
                        if start_duration_secs > 300:
                            raise Exception('%s has failed to start after %0.2f seconds' % (
                                job_name,
                                start_duration_secs,
                            ))
                    elif 'Running' == pod['status']['phase']:
                        # get all of the independent containers that are still alive (running or waiting)
                        live_cnt = 0
                        for cs in pod['status']['containerStatuses']:
                            if cs['name'] in dependent_containers:
                                pass
                            elif 'terminated' in cs['state']:
                                has_live_existed = True
                                exit_code = int(cs['state']['terminated'].get('exitCode', 0))
                                if exit_code > 0:
                                    raise Exception('%s has failed pods, failing task.' % job_name)
                            else:
                                live_cnt += 1

                        if live_cnt > 0:
                            has_live = True
                            break
                    elif 'Succeeded' == pod['status']['phase']:
                        # For us to end up in this block, the job has to be Running and the pod has to be Succeeded.
                        # This happens when (on a previous attempt) we successfully finished execution, killed dependent
                        # containers, and failed to delete the job.
                        # In this scenario, we want to immediately stop polling, and retry job deletion.
                        has_live_existed = True
                        has_live = False
                    else:
                        raise Exception(
                            "Encountered pod state {state} - no behavior has been prepared for pods in this state!".format(
                                state=pod["status"]["phase"]
                            )
                        )
                total_pods = len(pod_output['items'])
                logging.info("total pods: {total_pods}".format(total_pods=total_pods))
                has_live_existed = has_live_existed or has_live
                # if we get to this point but for some reason there are no pods, log it and retry
                if not has_live_existed:
                    logging.info('No pods have run. Retrying.')
                # we have no live pods, but live pods have existed.
                elif not has_live:
                    logging.info('No live, independent pods left.')
                    return pod_output
        finally:
            if pod_output:
                # let's clean up all our old pods. we'll kill the entry point (PID 1) in each running container
                for pod in pod_output.get('items', []):
                    # if we never got to running, there won't be containerStatuses
                    if 'containerStatuses' in pod['status']:
                        live_containers = [
                            cs['name']
                            for cs
                            in pod['status']['containerStatuses']
                            if 'running' in cs['state']
                        ]
                        for cname in live_containers:
                            logging.info('killing dependent live container %s' % cname)
                            # there is a race condition between reading the status and trying to kill the running
                            # container. ignore the return code to duck the issue.
                            subprocess.call(
                                namespaced_kubectl() + ['exec', pod['metadata']['name'], '-c', cname, 'kill', '1']
                            )

    def create_job_yaml(self, context):
        """
        create job_yaml_string from the operator's parameters

        :param context: Anything that implements xcom_pull
        :return: A tuple of the job's unique name and a string of YAML for the job
        """
        #
        unique_job_name = uniquify_job_name(self, context)

        # Copy the environment variables from the task and evaluate any XComs
        # Add in the AIRFLOW_xxx vars we need to support XComs from within the container
        instance_env = self.env.copy()
        instance_env['AIRFLOW_DAG_ID'] = self.dag_id
        instance_env['AIRFLOW_TASK_ID'] = self.task_id
        instance_env['AIRFLOW_ENVIRONMENT'] = configuration.get('core', 'environment')
        instance_env['AIRFLOW_EXECUTION_DATE'] = context['execution_date'].isoformat()
        instance_env['AIRFLOW_ENABLE_XCOM_PICKLING'] = configuration.getboolean('core', 'enable_xcom_pickling')
        instance_env['KUBERNETES_JOB_NAME'] = unique_job_name
        instance_env['AIRFLOW_MYSQL_HOST'] = '127.0.0.1'
        instance_env['AIRFLOW_MYSQL_DB'] = configuration.get('mysql', 'db')
        instance_env['AIRFLOW_MYSQL_USERNAME'] = KubernetesSecretParameter(
            secret_key_name='airflow-cloudsql-db-credentials',
            secret_key_key='username'
        )
        instance_env['AIRFLOW_MYSQL_PASSWORD'] = KubernetesSecretParameter(
            secret_key_name='airflow-cloudsql-db-credentials',
            secret_key_key='password'
        )
        if self.service_account_secret_name is not None:
            instance_env['GOOGLE_APPLICATION_CREDENTIALS'] = '/%s/key.json' % self.service_account_secret_name
        if configuration.getboolean('scheduler', 'statsd_on'):
            instance_env['STATSD_HOST'] = configuration.get('scheduler', 'statsd_host')
            instance_env['STATSD_PORT'] = configuration.get('scheduler', 'statsd_port')
            instance_env['STATSD_PREFIX'] = configuration.get('scheduler', 'statsd_prefix')

        cs_conns = [(configuration.get('mysql', 'cloudsql_instance'), 3306)]
        cs_next_port = 3307

        for cs_conn in self.cloudsql_connections:
            cs_conns.append((cs_conn.fully_qualified_instance, cs_next_port))
            instance_env[cs_conn.port_key] = cs_next_port
            cs_next_port += 1

        # Make a copy of all the containers.
        # Expand collections and apply XComs in args and/or command
        # Apply the instance environment variables
        # Add in the secrets volume
        instance_containers = [cs.copy() for cs in self.container_specs if cs['name'] != 'cloudsql-proxy']
        for cs in instance_containers:
            # all images should be stored in the triggeredmail container registry
            # if us.gcr.io/..., full path not given, grab from trigggeredmail container registry
            if 'us.gcr.io' not in cs['image']:
                cs['image'] = '%s/%s' % ('us.gcr.io/triggeredmail', cs['image'])
            if 'args' in cs:
                cs['args'] = list(map(str, enumerate_parameters(cs['args'], self, context=context)))
            if 'command' in cs:
                cs['command'] = list(map(str, enumerate_parameters(cs['command'], self, context=context)))
            # This assumes that env is a dictionary, which is possibly false
            cs['env'] = cs.get('env', {})
            cs['env'].update(instance_env)
            cs['env'] = dict_to_env(cs['env'], self, context=context)

            cs['volumeMounts'] = cs.get('volumeMounts', [])
            # do we already have the secret mount? if not, we should add it
            if self.service_account_secret_name is not None:
                if 0 == len([x for x in cs['volumeMounts'] if self.service_account_secret_name == x.get('name')]):
                    cs['volumeMounts'].append({
                        'name': self.service_account_secret_name,
                        'mountPath': "/%s" % self.service_account_secret_name,
                        'readOnly': True,
                    })

        instance_containers.append({
            'image': os.environ.get('AIRFLOW_GOOGLE_CLOUDSQL_PROXY_IMAGE', 'gcr.io/cloudsql-docker/gce-proxy:1.11'),
            'name': 'cloudsql-proxy',
            'command': [
                '/cloud_sql_proxy',
                '-instances=' + ','.join(['%s=tcp:%d' % x for x in cs_conns]),
                '-credential_file=/secrets/airflowcloudsql/credentials.json'],
            'env': [
                {'name': 'AIRFLOW_CONTAINER_LIFECYCLE', 'value': 'dependent'}
            ],
            'volumeMounts': [{
                'mountPath': '/secrets/airflowcloudsql',
                'name': 'airflow-cloudsql-instance-credentials',
                'readOnly': True}]
        })

        instance_volumes = [{
            'name': 'airflow-cloudsql-instance-credentials',
            'secret': {'secretName': 'airflow-cloudsql-instance-credentials'}
        }]

        if self.service_account_secret_name is not None:
            instance_volumes.append({
                'name': self.service_account_secret_name,
                'secret': {'secretName': self.service_account_secret_name},
            })

        skip_names = {v['name'] for v in instance_volumes}
        for v in self.volumes:
            if v['name'] not in skip_names:
                instance_volumes.append(v)

        kub_job_dict = {
            'apiVersion': 'batch/v1',
            'kind': 'Job',
            'metadata': {
                'name': unique_job_name,
                'namespace': 'airflow-{}'.format(configuration.get('core', 'environment_suffix'))
            },
            'spec': {
                'template': {
                    'spec': {
                        'containers': instance_containers,
                        'volumes': instance_volumes,
                        'restartPolicy': 'Never'
                    }
                },
                'backoffLimit': 0,
            },
        }

        return unique_job_name, yaml.safe_dump(kub_job_dict)

    @provide_session
    def execute(self, context, session=None):

        if self.die_if_duplicate:

            current_task_instance = TaskInstance(self, context['execution_date'])
            current_task_instance.refresh_from_db(include_queue_time=True)

            TI = TaskInstance
            instances_that_are_running = session.query(TI).filter(
                TI.dag_id == current_task_instance.dag_id,
                TI.task_id == current_task_instance.task_id,
                TI.state.in_([State.RUNNING, State.UP_FOR_RETRY, State.QUEUED]),
            ).all()

            should_die = False
            for task_instance in instances_that_are_running:
                if task_instance.queued_dttm < current_task_instance.queued_dttm:
                    should_die = True
                    break

            if should_die:
                raise Exception("A prior execution of this task is already running!  Failing this execution.")

        job_name, job_yaml_string = self.create_job_yaml(context)
        logging.info(job_yaml_string)
        self.instance_names.append(job_name)  # should happen once, but safety first!
        self.xcom_push(context, "kubernetes_job_name", job_name)

        with tempfile.NamedTemporaryFile(suffix='.yaml') as f:
            f.write(job_yaml_string)
            f.flush()
            result = subprocess.check_output(args=namespaced_kubectl() + ['apply', '-f', f.name])
            logging.info(result)

        # Setting pod_output to None, this will prevent a log_container_logs error
        # if polling fails and self.polling_job_completion is not able to return pod_output.
        pod_output = None

        try:
            pod_output = self.poll_job_completion(job_name)
            pod_output = pod_output or self.get_pods(job_name)  # if we didn't get it for some reason
            return None
        finally:
            try:
                # don't consider the job failed if this fails!
                self.log_container_logs(job_name, pod_output=pod_output)
            except Exception as ex:
                logging.error("Failed to process container logs: %s" % ex.message, extra={'err': ex})

            self.clean_up(job_name)
