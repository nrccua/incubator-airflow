# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from google.cloud import bigquery
from google.cloud.exceptions import BadRequest
import os
from airflow.utils.log.gcs_task_handler import GCSTaskHandler
from airflow import configuration
from datetime import datetime
import string
from airflow.contrib.utils.kubernetes_utils import retryable_check_output

try:
    import ujson as json
except ImportError:
    import json


class BQGCSTaskHandler(GCSTaskHandler):
    """
    BQGCSTaskHandler is a python log handler that handles and reads
    task instance logs. It extends airflow FileTaskHandler and GCSTaskHandler
    uploads to and reads from GCS remote storage. Upon log reading
    failure, it reads from host machine's local disk. The logs will be displayed in
    the airflow UI once retrieved from BigQuery.
    """

    def __init__(self, base_log_folder, gcs_log_folder, filename_template):
        super(BQGCSTaskHandler, self).__init__(base_log_folder, gcs_log_folder, filename_template)

    def _read(self, ti, try_number):
        """
        Read logs of given task instance and try_number from GCS.
        If failed, read the log from BigQuery.

        :param ti: task instance object
        :type ti: airflow.models.TypeInstance
        :param try_number: task instance try_number to read logs from
        :type try_number: int
        """
        # Explicitly getting log relative path is necessary as the given
        # task instance might be different than task instance passed in
        # in set_context method.
        log_relative_path = self._render_filename(ti, try_number + 1)
        remote_loc = os.path.join(self.remote_base, log_relative_path)

        if self.gcs_log_exists(remote_loc):
            # If GCS remote file exists, we do not fetch logs from task instance
            # local machine even if there are errors reading remote logs, as
            # remote_log will contain error message.
            remote_log = self.gcs_read(remote_loc, return_error=True)
            log = '*** Reading remote log from {}.\n{}\n'.format(
                remote_loc, remote_log)
            return log

        # everything above is copied from gcs_task_handler.py, the following elif statement includes changes
        # specifically for bluecore_gcs_task_handler
        elif ti.operator == "KubernetesJobOperator":
            # If remote file is not available and the job is a Kubernetes Job,
            # use the task instance attributes to query BigQuery logs and
            # make the logs available in real time from the "Log" page of the UI.

            # TODO: PROD-19910 job_name is always none on the first try, should fix this so an exception doesn't show each time
            job_name = ti.xcom_pull(task_ids=ti.task_id, key='kubernetes_job_name')
            if job_name is None:
                return "An XCOM kubernetes job_name was not found. This does not mean the job has failed. It is " \
                       "possible that a job_name has not yet been created. Try refreshing the page. "

            pod_output = BQGCSTaskHandler.get_pods(job_name)

            billing_project_id = configuration.get('core', 'kubernetes_bigquery_billing_project')
            client = bigquery.Client(billing_project_id)

            # get info for the BQ table name
            data_project_name = configuration.get('core', 'kubernetes_bigquery_data_project')
            start_date = ti.start_date.strftime("%Y%m%d")
            end_date = datetime.utcnow().date().strftime("%Y%m%d")

            log_lines = []
            tables = []

            for pod in pod_output['items']:
                pod_id = pod['metadata']['name']
                for container in pod['spec']['containers']:
                    container_name = string.replace(container['name'], '-', '_')
                    bq_table_name = "{}.gke_logs.{}_*".format(data_project_name, container_name)
                    tables.append((bq_table_name, pod_id))

            query = ("\n UNION ALL \n ".join(
                [BQGCSTaskHandler.generate_query(bq_table_name=bq_table_name, pod_id=pod_id, start_date=start_date,
                                                 end_date=end_date)
                 for
                 bq_table_name, pod_id in tables]) + "\n ORDER BY pod_id, logName, timestamp")

            try:
                query_job = client.query(query)
                result = query_job.result()
            except BadRequest as e:
                return "BadRequest error from BigQuery. The query may be empty or the job finished. Try refreshing " \
                       "the page. \n {e}".format(e=e)

            log_name = ""
            for index, row in enumerate(result):
                if row.logName.split("/")[-1:][0] != log_name:
                    log_name = row.logName.split("/")[-1:][0]
                    log_lines.append("LOGGING OUTPUT FROM {log_name} \n".format(log_name=log_name))
                log_lines.append("{}  {}".format(row.timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-4], row.textPayload))
            return "".join(log_lines)

        # else statement taken from gcs_task_handler.py
        else:
            log = super(BQGCSTaskHandler, self)._read(ti, try_number)
            return log

    # append is False if bluecore_bq_gcs.task so that the logs don't print twice
    def gcs_write(self, log, remote_log_location, append=False):
        """
        Writes the log to the remote_log_location. Fails silently if no hook
        was created.
        :param log: the log to write to the remote_log_location
        :type log: string
        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: string (path)
        :param append: if False, any existing log file is overwritten. If True,
            the new log is appended to any existing logs.
        :type append: bool
        """
        if append:
            old_log = self.gcs_read(remote_log_location)
            log = '\n'.join([old_log, log]) if old_log else log

        try:
            bkt, blob = self.parse_gcs_url(remote_log_location)
            from tempfile import NamedTemporaryFile
            with NamedTemporaryFile(mode='w+') as tmpfile:
                tmpfile.write(log)
                # Force the file to be flushed, since we're doing the
                # upload from within the file context (it hasn't been
                # closed).
                tmpfile.flush()
                self.hook.upload(bkt, blob, tmpfile.name)
        except Exception as e:
            self.log.error('Could not write logs to %s: %s', remote_log_location, e)

    @staticmethod
    def get_pods(job_name):
        """
        Return pods for a given job_name

        :param job_name: a unique job name
        :type job_name: string
        :return: result form kubectl command that contains job pod and container information
        :type: dict

        """
        return json.loads(retryable_check_output(args=[
            'kubectl', 'get', 'pods', '-o', 'json', '-l', 'job-name==%s' % job_name]))

    @staticmethod
    def generate_query(bq_table_name, pod_id, start_date, end_date):
        """
        Generates a query for the logs of a pod

        :param bq_table_name: name of the table that will be queried
        :type: string
        :param pod_id: name of the pod that the logging information is derived from
        :type pod_id: string
        :param start_date: task instance start date formatted by Ymd (20190901)
        :type start_date: string
        :param end_date: most recent date (now)
        :type end_date: string formatted by Ymd
        :return: string

        """
        query = (
            """
            SELECT logName, resource.labels.pod_id, timestamp, textPayload
            FROM `{bq_table_name}`
            WHERE resource.labels.pod_id = '{pod_id}'
            AND _TABLE_SUFFIX BETWEEN '{start_date}' AND '{start_date}'
            """).format(bq_table_name=bq_table_name,
                        pod_id=pod_id, start_date=start_date, end_date=end_date)
        return query
