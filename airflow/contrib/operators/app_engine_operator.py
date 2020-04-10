from airflow import configuration
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.utils.parameters import evaluate_xcoms, enumerate_parameters, enumerate_parameter_dict
from airflow.exceptions import AirflowException, AirflowTaskTimeout, AirflowConfigException
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator, XCOM_RETURN_KEY, TaskInstance
from airflow.utils.decorators import apply_defaults
from airflow.contrib.utils.kubernetes_utils import uniquify_job_name
from airflow.contrib.utils.xcom import try_xcom_pull
from datetime import datetime
import collections

try:
    import ujson as json
except ImportError:
    import json
import logging
import time
import yaml


class AppEngineOperator(BaseOperator):
    template_fields = ('command_params', 'job_id',)

    @apply_defaults
    def __init__(self,
                 task_id,
                 http_conn_id,
                 bucket,
                 command_params,
                 job_id,
                 google_cloud_conn_id='google_cloud_storage_default',
                 **kwargs):
        super(AppEngineOperator, self).__init__(task_id=task_id, **kwargs)
        self.http_conn_id = http_conn_id
        self.bucket = bucket
        command_params['job_id'] = job_id
        self.command_params = command_params
        self.job_id = job_id
        self.google_cloud_conn_id = google_cloud_conn_id

    def schedule_job(self):
        hook = HttpHook(
            method='POST',
            http_conn_id=self.http_conn_id)
        hook.run(
            endpoint='/api/airflow/schedule_job',
            headers={
                'content-type': 'application/json',
                'Accept': 'text/plain',
                'X-Airflow-Fernet-Key': configuration.get('core', 'fernet_key').strip(),
            },
            data=json.dumps(self.command_params),
            extra_options=None)

    def poll_status_files(self):
        success_file_name = '%s/succeeded' % self.job_id
        fail_file_name = '%s/failed' % self.job_id
        start_time = datetime.utcnow()
        i = 0
        # Bluecore App Engine backend instances timeout after an hour
        while (datetime.utcnow() - start_time).total_seconds() < 3600:
            time.sleep(min(60, 5 * 2 ** i))
            i += 1
            if check_gcs_file_exists(success_file_name, self.google_cloud_conn_id, self.bucket):
                return
            if check_gcs_file_exists(fail_file_name, self.google_cloud_conn_id, self.bucket):
                raise AirflowException('found failure file %s/%s' % (self.bucket, fail_file_name))

        raise AirflowTaskTimeout()

    def execute(self, context):
        # It seems that when an operator returns, it is considered successful,
        # and an operator fails if and only if it raises an AirflowException.
        # Good luck finding documentation saying that though.
        self.schedule_job()
        self.poll_status_files()


def check_gcs_file_exists(file_name, google_cloud_conn_id, bucket):
    hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=google_cloud_conn_id)
    return hook.exists(bucket, file_name)


# TODO Test schedule job works and fails
# TODO test find success file -> succeeds
# TODO test find fail file -> fail
# TODO test jinja render job id correctly


def safe_config_get(section, key):
    try:
        return configuration.get(section, key)
    except AirflowConfigException:
        return None


class AppEngineOperatorSync(BaseOperator):
    """
    AppEngineOperatorSync calls an API endpoint in App Engine and waits for a response. If the response has a 4xx or 5xx
    status, the task is considered failed. If a body is included in the response, it will be stored in the
    `return_value` XCom. JSON and YAML will be deserialized automatically.

    :param task_id: Name of the task to appear in Airflow UI
    :type task_id: str
    :param command_name: Full name of the App Engine command to be called (e.g. engine.core.commands.ExCommand)
    :type command_name: str
    :param command_params: Named parameters of App Engine command to be called
    :type command_params: dict
    :param http_conn_id: ID mapped to the host to which the call is sent. Can be defined in Airflow UI
    :type http_conn_id: str
    :param kwargs: Named parameters to pass to BaseOperator constructor
    :type kwargs: dict
    """
    template_fields = ('command_name', 'command_params')

    @apply_defaults
    def __init__(self,
                 task_id,
                 command_name,
                 command_params={},
                 http_conn_id='appengine',
                 **kwargs):
        super(AppEngineOperatorSync, self).__init__(task_id=task_id, **kwargs)
        self.http_conn_id = http_conn_id
        self.command_name = command_name
        self.command_params = command_params

    def execute(self, context):
        hook = HttpHook(
            method='POST',
            http_conn_id=self.http_conn_id)

        headers = {
            'content-type': 'application/json',
            'Accept': 'application/json',
            'X-Bluecore-Token': configuration.get('appengine', 'token').strip(),
            # these are not necessary, but may make debugging easier later
            'X-Airflow-Dag-Id': self.dag_id,
            'X-Airflow-Task-Id': self.task_id,
            'X-Airflow-Execution-Date': context['execution_date'].isoformat(),
            'X-Airflow-Fernet-Key': configuration.get('core', 'fernet_key').strip(),
        }

        mysql_host = safe_config_get('mysql', 'host')
        if mysql_host is not None:
            headers['X-Airflow-Mysql-Host'] = mysql_host

        mysql_cloudsql_instance = safe_config_get('mysql', 'cloudsql_instance')
        if mysql_cloudsql_instance is not None:
            headers['X-Airflow-Mysql-Cloudsql-Instance'] = mysql_cloudsql_instance

        instance_params = enumerate_parameters(self.command_params, self, context)

        # this will throw on any 4xx or 5xx
        with hook.run(
            endpoint='/api/airflow_v2/sync/%s' % self.command_name,
            headers=headers,
            data=json.dumps(instance_params),
            extra_options=None
        ) as response:
            if response.content:
                # be careful of content types with an encoding suffix
                content_type = response.headers['Content-Type'].split(';')[0]
                if content_type == 'application/json':
                    body = response.json()
                elif content_type == 'text/plain':
                    body = response.text
                elif content_type in {
                    'text/yaml', 'text/x-yaml', 'text/vnd.yaml', 'application/yaml', 'application/x-yaml'
                }:
                    body = yaml.load(response.text)
                else:
                    body = response.content

                self.xcom_push(context=context, key=XCOM_RETURN_KEY, value=body)


# thank you stack overflow
def convert(data):
    if isinstance(data, basestring):
        return str(data)
    elif isinstance(data, collections.Mapping):
        return dict(map(convert, data.items()))
    elif isinstance(data, collections.Iterable):
        return type(data)(map(convert, data))
    else:
        return data


class AppEngineOperatorAsync(BaseOperator):
    """
    AppEngineOperatorAsync schedules a command on the App Engine task queue. Task completion is signalled by setting
    the `return_value` in the command. If the return value is not set by the command, this will time out after an hour.

    :param task_id: Name of the task to appear in Airflow UI
    :type task_id: str
    :param command_name: Full name of the App Engine command to be called (e.g. engine.core.commands.ExCommand)
    :type command_name: str
    :param appengine_queue: Name of the App Engine task queue where this command will be enqueued
    :type appengine_queue: str
    :param command_params: Named parameters of App Engine command to be called
    :type command_params: dict
    :param http_conn_id: ID mapped to the host to which the call is sent. Can be defined in Airflow UI
    :type http_conn_id: str
    :param appengine_timeout: Number of seconds passed without a response in xcom before throwing a AirflowTaskTimeout
    :type appengine_timeout: int
    :param kwargs: Named parameters to pass to BaseOperator constructor
    :type kwargs: dict
    """
    template_fields = ('command_name', 'command_params')

    @apply_defaults
    def __init__(self,
                 task_id,
                 command_name,
                 appengine_queue,
                 command_params={},
                 http_conn_id='appengine',
                 appengine_timeout=3600,
                 **kwargs):
        super(AppEngineOperatorAsync, self).__init__(task_id=task_id, **kwargs)
        self.http_conn_id = http_conn_id
        self.appengine_timeout = appengine_timeout
        self.command_name = command_name
        self.command_params = command_params
        self.appengine_queue = appengine_queue
        self.should_adopt = False
        self.finished = False
        self.return_value = None
        self.exc_message = None
        self.exc_type = None
        self.exc_callstack = None

    def retrieve_exception_details(self, context):
        self.exc_message = self.safe_xcom_pull(
            context=context,
            task_ids=self.task_id,
            key='__EXCEPTION_MESSAGE'
        )

        self.exc_type = self.safe_xcom_pull(
            context=context,
            task_ids=self.task_id,
            key='__EXCEPTION_TYPE'
        )

        self.exc_callstack = self.safe_xcom_pull(
            context=context,
            task_ids=self.task_id,
            key='__EXCEPTION_CALLSTACK'
        )

    def save_previous_xcoms(self, context):
        pending_tuple = try_xcom_pull(
            context=context,
            task_ids=self.task_id,
            key='is_pending_{}'.format(TaskInstance(self, context['execution_date']).try_number)
        )
        if pending_tuple[0]:
            self.should_adopt = True

        return_value_tuple = try_xcom_pull(
            context=context,
            task_ids=self.task_id
        )
        if return_value_tuple[0]:
            self.finished = True
            self.return_value = return_value_tuple[1]
            if self.return_value == '__EXCEPTION__':
                self.retrieve_exception_details(context)

    def schedule_job(self, context):
        current_task_instance = TaskInstance(self, context['execution_date'])
        current_task_instance.refresh_from_db()
        try_number = current_task_instance.try_number

        if self.should_adopt:
            logging.info("Job is already scheduled - skipping to polling phase.")
            return

        logging.info("Job was not already scheduled - executing schedule phase.")

        hook = HttpHook(
            method='POST',
            http_conn_id=self.http_conn_id)

        headers = {
            'content-type': 'application/json',
            'Accept': 'text/plain',
            'X-Bluecore-Token': configuration.get('appengine', 'token').strip(),
            'X-Airflow-Dag-Id': self.dag_id,
            'X-Airflow-Task-Id': self.task_id,
            'X-Airflow-Execution-Date': context['execution_date'].isoformat(),
            'X-Airflow-Enable-Xcom-Pickling': str(configuration.getboolean('core', 'enable_xcom_pickling')),
            'X-Airflow-Mysql-Db': configuration.get('mysql', 'db').strip(),
            'X-Airflow-Mysql-User': configuration.get('mysql', 'username').strip(),
            'X-Airflow-Mysql-Password': configuration.get('mysql', 'password').strip(),
            'X-Airflow-Fernet-Key': configuration.get('core', 'fernet_key').strip(),
        }

        mysql_host = safe_config_get('mysql', 'host')
        if mysql_host is not None:
            headers['X-Airflow-Mysql-Host'] = mysql_host

        mysql_cloudsql_instance = safe_config_get('mysql', 'cloudsql_instance')
        if mysql_cloudsql_instance is not None:
            headers['X-Airflow-Mysql-Cloudsql-Instance'] = mysql_cloudsql_instance
            headers['X-Airflow-Mysql-Cloudsql-Instance'] = mysql_cloudsql_instance

        # generate a unique job name for the command to be added to the App Engine task queue
        job_id = uniquify_job_name(self, context)
        logging.info("Job ID: %s", job_id)

        logging.error("command params are {}".format(self.command_params))
        logging.error("context is {}".format(context))
        self.command_params = convert(self.command_params)

        instance_params = evaluate_xcoms(self.command_params, self, context)
        logging.error("instance params are {}".format(instance_params))

        try:
            test_params = evaluate_xcoms(self.command_params, self, context)
            logging.error("testing params {}".format(test_params))
        except Exception as e:
            logging.error(e)

        try:
            enum_params = enumerate_parameters(self.command_params, self, context)
            logging.info("enum params are {}".format(enum_params))
        except Exception as e:
            logging.error(e)

        post_data = {'params_dict': instance_params, 'appengine_queue': self.appengine_queue, 'job_id': job_id,
                     'try_number': try_number}

        logging.info("Monolith responded with: {}".format(str(
            hook.run(
                endpoint='/api/airflow_v2/async/%s' % self.command_name,
                headers=headers,
                data=json.dumps(post_data),
                extra_options=None
            ).text
        )))

    def safe_xcom_pull(self, context, task_ids, dag_id=None, key=XCOM_RETURN_KEY, include_prior_dates=None):
        """
        Wraps the existing xcom_pull method, but returns None if there is any exception.
        :param context:
        :param task_ids:
        :param dag_id:
        :param key:
        :param include_prior_dates:
        :return:
        """
        try:
            return self.xcom_pull(
                context=context,
                task_ids=task_ids,
                dag_id=dag_id,
                key=key,
                include_prior_dates=include_prior_dates)
        except:
            return None

    def poll_status(self, context):

        retval = None

        if self.finished:
            logging.info("Job is already finished - skipping to result phase.")
            retval = self.return_value
        else:
            logging.info("Job is not finished - executing poll phase.")
            start_time = datetime.utcnow()
            i = 0
            # Bluecore App Engine backend instances timeout after an hour
            while retval is None:
                remaining_secs = self.appengine_timeout - (datetime.utcnow() - start_time).total_seconds()
                logging.info("%0.2f seconds remain until timeout" % remaining_secs)
                if remaining_secs <= 0:
                    raise AirflowTaskTimeout()

                # try_xcom_pull allows us to distinguish between cases where the task
                # hasn't pushed an XCom and where the task pushed an XCom with value None.
                retval_tuple = try_xcom_pull(context=context, task_ids=self.task_id)
                # if XCom not yet pushed
                if not retval_tuple[0]:
                    logging.info("XCom response not found. Sleeping.")
                    # sleep for a while and try again
                    time.sleep(min(60, 2 ** i))
                    i += 1
                    continue
                logging.info("XCom response received: %s" % str(retval))
                retval = retval_tuple[1]

                if retval == '__EXCEPTION__':
                    self.retrieve_exception_details(context)
                break

        logging.info("Executing result phase.")
        if retval == '__EXCEPTION__':
            logging.error(
                "Found exception %s: %s" %
                (self.exc_type or '<UNKNOWN>', self.exc_message or '<UNKNOWN>')
            )

            if self.exc_callstack:
                logging.error(str(self.exc_callstack))

            raise AirflowException(self.exc_message)

        logging.info("Remote task finished successfully.")
        return

    def execute(self, context):
        # TODO I think we can delete this comment - it doesn't appear to reference anything anymore.
        # It seems that when an operator returns, it is considered successful,
        # and an operator fails if and only if it raises an AirflowException.
        # Good luck finding documentation saying that though.
        self.schedule_job(context)
        try:
            self.poll_status(context)
        except:
            logging.error('something horrible has happenend')
            raise
        finally:
            logging.info("Completed execute")
