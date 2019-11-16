from airflow.models import XCom, XCOM_RETURN_KEY
from airflow.utils.db import provide_session
from airflow import configuration
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.helpers import is_container
from sqlalchemy import and_
import pickle
import json
import functools


@provide_session
def try_get_one(
            execution_date,
            key=None,
            task_id=None,
            dag_id=None,
            include_prior_dates=False,
            enable_pickling=None,
            session=None):
    """
    Retrieve an XCom value, optionally meeting certain criteria.
    TODO: "pickling" has been deprecated and JSON is preferred. "pickling" will be removed in Airflow 2.0.

    :param enable_pickling: If pickling is not enabled, the XCOM value will be parsed to JSON instead.
    :return: XCom value
    """
    filters = []
    if key:
        filters.append(XCom.key == key)
    if task_id:
        filters.append(XCom.task_id == task_id)
    if dag_id:
        filters.append(XCom.dag_id == dag_id)
    if include_prior_dates:
        filters.append(XCom.execution_date <= execution_date)
    else:
        filters.append(XCom.execution_date == execution_date)

    query = (
        session.query(XCom.value)
        .filter(and_(*filters))
        .order_by(XCom.execution_date.desc(), XCom.timestamp.desc()))

    result = query.first()
    if result:
        if enable_pickling is None:
            enable_pickling = configuration.getboolean('core', 'enable_xcom_pickling')

        if enable_pickling:
            return (True, pickle.loads(result.value))
        else:
            try:
                return (True, json.loads(result.value.decode('UTF-8')))
            except ValueError:
                log = LoggingMixin().log
                log.error("Could not serialize the XCOM value into JSON. "
                          "If you are using pickles instead of JSON "
                          "for XCOM, then you need to enable pickle "
                          "support for XCOM in your airflow config.")
                raise
    return (False, None)


def try_xcom_pull(
        context,
        task_ids,
        dag_id=None,
        key=XCOM_RETURN_KEY,
        include_prior_dates=False):
    """
    Pull XComs that optionally meet certain criteria.

    The default value for `key` limits the search to XComs
    that were returned by other tasks (as opposed to those that were pushed
    manually). To remove this filter, pass key=None (or any desired value).

    If a single task_id string is provided, the result is a tuple (True, val)
    where val is the value of the most recent matching XCom from that task_id.
    If multiple task_ids are provided, a tuple of matching values is returned.
    Tuple (False, None) is returned whenever no matches are found.

    :param key: A key for the XCom. If provided, only XComs with matching
        keys will be returned. The default key is 'return_value', also
        available as a constant XCOM_RETURN_KEY. This key is automatically
        given to XComs returned by tasks (as opposed to being pushed
        manually). To remove the filter, pass key=None.
    :type key: string
    :param task_ids: Only XComs from tasks with matching ids will be
        pulled. Can pass None to remove the filter.
    :type task_ids: string or iterable of strings (representing task_ids)
    :param dag_id: If provided, only pulls XComs from this DAG.
        If None (default), the DAG of the calling task is used.
    :type dag_id: string
    :param include_prior_dates: If False, only XComs from the current
        execution_date are returned. If True, XComs from previous dates
        are returned as well.
    :type include_prior_dates: bool
    """
    if dag_id is None:
        dag_id = context['ti'].dag_id

    pull_fn = functools.partial(
        try_get_one,
        execution_date=context['ti'].execution_date,
        key=key,
        dag_id=dag_id,
        include_prior_dates=include_prior_dates)

    if is_container(task_ids):
        return tuple(pull_fn(task_id=t) for t in task_ids)
    else:
        return pull_fn(task_id=task_ids)
