import ast
import jinja2
from airflow import configuration
from collections import namedtuple
from datetime import datetime
import hashlib
import re
import subprocess
import logging


DEFAULT_YAML_TEMPLATE = """
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ job_name }}
spec:
  template:
    spec:
      containers: {% for container in containers %}
      - name: {{ container.name }}
        image: {{ container.image }}
        command: {{ container.command }}
        volumeMounts:
        - name: {{ service_account_secret_name }}
          mountPath: /{{ service_account_secret_name }}
          readOnly: true
        env: {% for name, value in env.iteritems() %}
        - name: {{ name }}
          value: {{ value }}{% endfor %}{% endfor %}
      volumes:
      - name: {{ service_account_secret_name }}
        secret:
          secretName: {{ service_account_secret_name }}
      restartPolicy: Never
  backoffLimit: 0
"""


def retryable_check_output(args, retry_count=3):
    """
    Reads the job description, retrying on failure
    :param args: Arguments to pass to subprocess.check_output
    :type args: List of string
    :param retry_count: Number of times to retry (default=3)
    :type retry_count: int
    :return: string
    """
    try:
        return subprocess.check_output(args=args)
    except subprocess.CalledProcessError as e:
        if retry_count > 0:
            logging.info("Retrying check_output because %s" % e)
            return retryable_check_output(args=args, retry_count=retry_count - 1)
        else:
            raise


def generate_yaml(kubernetes_job_yaml_dictionary):
    """
    Generate YAML string from a Kubernetes Job yaml template
    and provided values.

    """
    template = jinja2.Template(DEFAULT_YAML_TEMPLATE)
    return template.render(kubernetes_job_yaml_dictionary)


def generate_kubernetes_job_yaml(job_name,
                                 container_information_list,
                                 service_account_secret_name,
                                 env=None):
    """
    Creates a Kubernetes Job yaml from a Jinja template.
    Will ensure that the job name is unique,
    avoiding jobs overwriting each other.
    Kubernetes secret being used must have the service account
    keyfile json stored as key.json.
    """
    env = env or {}
    env['GOOGLE_APPLICATION_CREDENTIALS'] = '/%s/key.json' % service_account_secret_name
    job_yaml_dictionary = {
        'job_name': job_name,
        'containers': container_information_list,
        'service_account_secret_name': service_account_secret_name,
        'env': env,
    }
    template = jinja2.Template(DEFAULT_YAML_TEMPLATE)
    return template.render(job_yaml_dictionary)


class KubernetesContainerInformation(object):
    """
    Information for an individual container,
    used to generate Kubernetes Job yamls.
    """

    def __init__(self,
                 name,
                 image,
                 command=None,
                 args=None):
        self.name = name
        self.image = image
        self.command = \
            KubernetesContainerInformation.unknown_to_array(command)
        self.args = \
            KubernetesContainerInformation.unknown_to_array(args)

    @staticmethod
    def unknown_to_array(value):
        from airflow.utils.helpers import is_container
        if value is None or len(value) == 0:
            return None

        if isinstance(value, basestring):
            if value[0] == '[' and value[-1] == ']':
                return ast.literal_eval(value)
            else:
                return [value]
        elif is_container(value):
            return value
        else:
            raise ValueError('input was not array or string or string representing an array')

    def to_dict(self):
        ret = dict(name=self.name, image=self.image)
        if self.command is not None:
            ret['command'] = self.command
        if self.args is not None:
            ret['args'] = self.args

        return ret


class KubernetesSecretParameter(object):
    def __init__(self, secret_key_name, secret_key_key):
        self.secret_key_name = secret_key_name
        self.secret_key_key = secret_key_key

def namespaced_kubectl():
    return [
        "kubectl",
        "--namespace",
        'airflow-{}'.format(configuration.get('core', 'environment_suffix')),
    ]


def dict_to_env(source, task_instance, context=None):
    """
    Converts an incoming dictionary into a list of name:value dictionaries, as
    is used in the "env" member of a container in Kubernetes YAML. Will expand
    XComParameter instances, as well. Take caution when providing multi-task
    XComParameter values or multi-item collections. Environment variables only
    support a single value for each key, so the behavior in multi-set conditions
    is undefined. Also, if the key is not a string this will raise a ValueError.

    :param source: Dict-like object, mapping string:string or string:XComParameter
    :param task_instance: Source of xcom_pull
    :param context: Optional context to pass when when giving an operator
                    instead of a task instance
    :return: list of name:value dictionaries
    """
    from airflow.contrib.utils.parameters import enumerate_parameters

    retval = []
    for k, v in source.iteritems():
        if not isinstance(k, basestring):
            raise ValueError("Key was not a string")

        if isinstance(v, KubernetesSecretParameter):
            retval.append({
                'name': k,
                'valueFrom': {
                    'secretKeyRef': {'name': v.secret_key_name, 'key': v.secret_key_key}
                }})
        else:
            # we may receive dicts to be interpreted by Kubernetes. don't mess with those
            if isinstance(v, dict):
                inner = v
            else:
                # support XComs and such; environment variables can only have one value.
                inner = str(reduce((lambda x, y: y or x), enumerate_parameters(v, task_instance, context=context)))
            if inner:
                retval.append({'name': k, 'value': inner})
    return retval


def uniquify_job_name(task, context, job_name=None):
    """
    uniquify_job_name generates a unique name for each job based on the
    job name appended with some magic!

    :param task: The task for which you want a unique name.
    :param context: An Airflow context. Must have ['execution_date'] datetime
           member.
    :param job_name: A name that we will be uniquifying. If passing a KubernetesJobOperator instance or an
           AppEngineAsyncOperator, the job_name will be inferred.
    :return: A unique string for the task instance
    """
    if job_name is None:
        if hasattr(task, 'job_name'):
            job_name = task.job_name
        elif hasattr(task, 'command_name'):
            job_name = task.command_name.split('.')[-1]


    logging.info(
        "OLD BEN:\njob_name is {}\nexecution date is {}\ndag_id is {}\ntask_id is {}\ngenerated name is {}".format(
            job_name, context['execution_date'].isoformat(), task.dag_id, task.task_id,
            "-".join([
                job_name,
                hashlib.sha512(" ".join([
                    context['execution_date'].isoformat(),
                    task.dag_id,
                    task.task_id,
                ]).encode('utf-8')).hexdigest()[:16]
            ])
        )
    )
    return "-".join([
        job_name,
        hashlib.sha512(" ".join([
            context['execution_date'].isoformat(),
            task.dag_id,
            task.task_id,
            # To compute the probability of a hash collision (p) after retrieving a number of hashes (n) where H is the
            # number of possible hashes, you can use:
            #
            # p = (n^2) / 2H
            #
            # https://www.ilikebigbits.com/2018_10_20_estimating_hash_collisions.html
            #
            # H is 16^d, where d is the number of characters in our truncated hash, so we get:
            # p = n^2 / 2*(16^d)
            #
            # Let's say a job can run every hour for 100 years, making n=876,000:
            #
            # p = 876000^2 / 2*(16^d)
            # for d=16, p=2.08e-8
            # for d=32, p=1.13e-27
            # So we are satisfied with truncating our hash to 16 characters.
        ])).hexdigest()[:16]
    ])


def deuniquify_job_name(unique_job_name):
    """
    Strips all the magic from a job name made unique by uniquify_job_name
    :param unique_job_name: Name with unique garbage on it
    :return: Name without unique garbage
    """
    return re.sub('^(.+)-[0-9a-f]{16}-[0-9a-f]{12,16}$', '\\1', unique_job_name)


# Information for additional CloudSQL connections to open in the proxy. The KubernetesJobOperator will assign
# a TCP port for each connection, putting the value of that selection into the environment variable specified
# by port_key. Consumers are expected to read that environment variable when making their connection. E.g. if
# you specify `port_key=MY_DB_PORT` here, in your operator you should connect using
# `MySQLdb.connect(..., port=os.environ['MY_DB_PORT'])`
#
# For the connection to work, the service account used by the Cloud SQL Proxy (airflow-cloudsql-instance-credentials)
# must have access to talk to the target database.
#
# :param fully_qualified_instance: project:region:name to connect to
# :param port_key: name of environment variable where the connection port should be found
CloudSQLConnection = namedtuple('CloudSQLConnection', ['fully_qualified_instance', 'port_key'])
