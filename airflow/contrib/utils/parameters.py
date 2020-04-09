from __future__ import absolute_import
import logging
from airflow.models import BaseOperator
try:
    import ujson as json
except ImportError:
    import json


class XComParameter(object):
    """
    A parameter value that will be read from an XCom on execution of a task.

    In the case of the kubernetes_operator, these will be converted into one or
    more command line parameters in the script invocation. For AppEngine, these
    will be sent... somehow to be determined later. Options include delimited
    GET params, multiple GET params, or POST body.
    """
    def __init__(self, task_ids, xcom_key='return_value'):
        """
        Makes a new XComParameter

        :param task_ids: a string representing task_id or a list of them
        :param xcom_key: the key to read. defaults to 'return_value'
        """
        self.task_ids = task_ids
        self.key = xcom_key

    def get_values(self, ti, context=None):
        """
        Gets a list of parameter values

        :param ti: A TaskInstance or other object implementing xcom_pull
        :param context: Optional context to pass when when giving an operator
                        instead of a task instance
        :return: A container (list or tuple) with each returned value in the
                 same order as the task_ids provided in the constructor. If a
                 task_id has no value, it will be None.
        """
        params = dict(task_ids=self.task_ids, key=self.key)
        if isinstance(ti, BaseOperator):
            if context is None:
                raise ValueError("context is required when using BaseOperator instead of TaskInstance")
            params['context'] = context
        if hasattr(self.task_ids, '__iter__') and not isinstance(self.task_ids, basestring):
            return ti.xcom_pull(**params)
        else:
            return [ti.xcom_pull(**params)]

    def get_value(self, ti, context=None):
        """
        Gets a list of parameter values if a list of task_ids was provided, otherwise a bare value

        :param ti: A TaskInstance or other object implementing xcom_pull
        :param context: Optional context to pass when when giving an operator
                        instead of a task instance
        :return: A container (list or tuple) with each returned value in the
                 same order as the task_ids provided in the constructor, if a list were provided.
                 If a single task_id was given, a single value will be returned. If a task_id has
                 no value, it will be None.
        """
        params = dict(task_ids=self.task_ids, key=self.key)
        if isinstance(ti, BaseOperator):
            if context is None:
                raise ValueError("context is required when using BaseOperator instead of TaskInstance")
            params['context'] = context
        if hasattr(self.task_ids, '__iter__') and not isinstance(self.task_ids, basestring):
            return ti.xcom_pull(**params)
        else:
            return ti.xcom_pull(**params)


class SerializeMe(object):
    """
    A parameter value that will evaluate all nested XComParameters and then serialize the source
    (for now only supports json).

    This means that the image expecting to receive this command line argument should then
    deserialize.
    """
    def __init__(self, parameter_object):
        """
        Makes a new SerializeMe object

        :param parameter_object: an object to be serialized after any nested XComParameter values
        are evaluated
        """
        self.parameter_object = parameter_object


def enumerate_parameter_dict(source_dict, task_instance, context=None):
    for key, value in source_dict.items():
        if isinstance(value, dict):
            for inner_key, inner_value in value.iteritems():
                for iiv in enumerate_parameters(inner_value, task_instance=task_instance, context=context):
                    yield (key, '{}={}'.format(inner_key, iiv))
        elif hasattr(value, '__iter__') and not isinstance(value, basestring):
            for v in value:
                yield(key, v)
        elif isinstance(value, XComParameter):
            for v in value.get_values(task_instance, context=context):
                if v is not None:
                    yield (key, v)
        else:
            yield (key, value)


def enumerate_parameters(source, task_instance, context=None):
    """
    Flatten inputs, evaluate XComs, be horrible.

    If you are passing in keyed elements with keys of non-string types, you're a jerk.

    :param source: A thing, singular or iterable, XCom, or SerializeMe
    :param task_instance: A thing that can decode XComs
    :param context: Optional context to pass when when giving an operator
                    instead of a task instance
    :return: A generator of all the things, flattened
    """
    if source is None:
        return

    if task_instance and not hasattr(task_instance, 'xcom_pull'):
        raise ValueError("Provided task_instance object does have the xcom_pull method")

    if isinstance(source, (basestring, bool, int, long, float)):
        yield source
    elif isinstance(source, XComParameter):
        for v in source.get_values(task_instance, context=context):
            if v is not None:
                yield v
    elif isinstance(source, SerializeMe):
        yield json.dumps(evaluate_xcoms(source.parameter_object, task_instance, context))
    elif hasattr(source, "keys"):
        for k, v in enumerate_parameter_dict(source, task_instance, context=context):
            if v is not None:
                yield k
                yield v
    elif hasattr(source, "__iter__"):
        if len(source) == 2:
            for t in enumerate_parameters(source[1], task_instance, context=context):
                if t is not None:
                    yield source[0]
                    yield t
        else:
            for inner_value in source:
                for t in enumerate_parameters(inner_value, task_instance, context=context):
                    if t is not None:
                        yield t
    elif hasattr(source, "__str__"):
        yield source.__str__()
    else:
        raise TypeError(
            "Type %s.%s of value %s not supported" % (
                source.__class__.__module__,
                source.__class__.__name__,
                source,
            ))


def evaluate_xcoms(source, task_instance, context=None):
    """
    Takes in an object (or container) and returns the same thing but with XComParameters evaluated

    :param source: The object (or container) that (may) have XComParameters
    :param task_instance: The task instance whose asking
    :param context: Optional context to pass when when giving an operator
                    instead of a task instance
    :return: A data structure that looks (mostly) like `source`, but with XComParameters evaluated
    """
    if source is None:
        return

    if task_instance and not hasattr(task_instance, 'xcom_pull'):
        raise ValueError("Provided task_instance object does have the xcom_pull method")

    if isinstance(source, (basestring, bool, int, long, float)):
        if 'XComParameter' in source:
            logging.error("found XComParameter string in source.")
            try:
                logging.error("eval(source) is {}".format(eval(source)))
                evaluate_xcoms(eval(source), task_instance, context)
            except Exception as e:
                logging.error(e)
        logging.error("returning source bc basestring, bool, int, long, float")
        return source
    elif isinstance(source, XComParameter):
        logging.error("XComParameter is instance")
        return source.get_value(task_instance, context)
    elif hasattr(source, "keys"):
        logging.error("source has keys")
        retval = {}
        for k, v in source.items():
            logging.error("evaluating source")
            retval[k] = evaluate_xcoms(v, task_instance, context)
            logging.error("retval[{}] is {}".format(k, retval[k]))
        logging.error("retval is {}".format(retval))
        return retval
    elif hasattr(source, "__iter__"):
        return [evaluate_xcoms(x, task_instance, context) for x in source]
    else:
        return source
