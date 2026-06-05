"""
EPMT list command module - handles job listing functionality.
"""

from sys import stderr
from logging import getLogger

from pandas import DataFrame

from epmt.epmt_query import ( get_unanalyzed_jobs, get_unprocessed_jobs, get_jobs, get_procs, get_refmodels,
                              get_thread_metrics, get_job_proc_tags, get_op_metrics )
from epmt.epmtlib import kwargify

logger = getLogger(__name__)


def epmt_list(arglist):
    '''
    Dispatch list sub-commands based on the first element of arglist.

    Supported sub-commands: jobs, unprocessed_jobs, unanalyzed_jobs,
    refmodels, procs/processes, thread_metrics, op_metrics, job_proc_tags.
    Defaults to listing jobs when arglist is empty or unrecognized.
    '''
    logger.info("epmt_list: %s", str(arglist))
    if not arglist:
        return epmt_list_jobs(arglist)
    if arglist[0] == "jobs":
        arglist = arglist[1:]
        return epmt_list_jobs(arglist)
    if arglist[0] == "unprocessed_jobs":
        arglist = arglist[1:]
        return epmt_list_unprocessed_jobs(arglist)
    if arglist[0] == "unanalyzed_jobs":
        arglist = arglist[1:]
        return epmt_list_unanalyzed_jobs(arglist)
    if arglist[0] == "refmodels":
        arglist = arglist[1:]
        return epmt_list_refmodels(arglist)
    if arglist[0] == "procs" or arglist[0] == "processes":
        arglist = arglist[1:]
        return epmt_list_procs(arglist)
    if arglist[0] == "thread_metrics":
        arglist = arglist[1:]
        return epmt_list_thread_metrics(arglist)
    if arglist[0] == "op_metrics":
        arglist = arglist[1:]
        return epmt_list_op_metrics(arglist)
    if arglist[0] == "job_proc_tags":
        arglist = arglist[1:]
        return epmt_list_job_proc_tags(arglist)
    return epmt_list_jobs(arglist)


def epmt_list_unanalyzed_jobs(arglist):
    '''
    List jobs that have been submitted but not yet analyzed.

    If arglist contains specific job IDs, verifies they are all unanalyzed
    and warns about any not found. Prints the list and returns True on
    success, False if no unanalyzed jobs exist or specified jobs are missing.
    '''
    logger.info("epmt_list_unanalyzed_jobs: %s", str(arglist))
    jobs = get_unanalyzed_jobs(jobs=arglist)
    if len(jobs) == 0:
        logger.warning("get_list_unanalyzed_jobs: no unanalyzed jobs")
        if len(arglist):
            return False
        return True

    if len(arglist):
        jobids_in = set()
        jobids = set()
        jobids_in.update(arglist)
        jobids.update(jobs)
        leftover = jobids_in.difference(jobids)
        if len(leftover):
            logger.warning("Unanalyzed jobs not found: %s", str(leftover))
            return False

    print(jobs)
    return True


def epmt_list_unprocessed_jobs(arglist):
    '''
    List jobs that are in the database but have not been processed.

    If arglist contains specific job IDs, verifies they are all unprocessed
    and warns about any not found. Prints the list and returns True on
    success, False if no unprocessed jobs exist or specified jobs are missing.
    '''
    logger.info("epmt_list_unprocessed_jobs: %s", str(arglist))
    jobs = get_unprocessed_jobs()
    if len(jobs) == 0:
        logger.warning("get_list_unprocessed_jobs: no unprocessed jobs in table")
        if len(arglist):
            return False
        return True

    if len(arglist):
        jobids_in = set()
        jobids = set()
        jobids_in.update(arglist)
        jobids.update(jobs)
        leftover = jobids_in.difference(jobids)
        if len(leftover):
            logger.warning("Jobs not found in unprocessed table: %s", str(leftover))
            return False

    print(jobs)
    return True


def epmt_list_jobs(arglist):
    '''
    List jobs stored in the database.

    Parses arglist as keyword arguments and passes them to get_jobs.
    Defaults to terse output format when no fmt kwarg is provided.
    '''
    logger.info("epmt_list_jobs: %s", str(arglist))
    kwargs = kwargify(arglist)
    if kwargs.get('fmt') is None:
        kwargs['fmt'] = 'terse'
    jobs = get_jobs(**kwargs)

    print(jobs)
    return True


def epmt_list_procs(arglist):
    '''
    List process records from the database.

    Parses arglist as keyword arguments and passes them to get_procs.
    Returns False if no processes are found, True otherwise.
    '''
    logger.info("epmt_list_jobs: %s", str(arglist))
    kwargs = kwargify(arglist)
    jobs = get_procs(**kwargs)
    if len(jobs) == 0:
        logger.info("get_procs %s returned no processes", str(kwargs))
        return False
    print(jobs)
    return True


def epmt_list_thread_metrics(arglist):
    '''
    List thread-level metrics for the given process IDs.

    arglist should contain integer process IDs. Returns False if no
    thread metrics are found for the given IDs, True otherwise.
    '''
    logger.info("epmt_list_thread_metrics: %s", str(arglist))
    arglist = list(map(int, arglist))
    tm = get_thread_metrics(arglist)
    if tm.empty:
        logger.info("get_thread_metrics %s returned no thread metrics", str(arglist))
        return False
    print(tm)
    return True


def epmt_list_op_metrics(arglist):
    '''
    List operation-level metrics for specified jobs.

    arglist must be non-empty and is parsed as keyword arguments passed
    to get_op_metrics. Returns False if arglist is empty or no op metrics
    are found, True otherwise.
    '''
    if not arglist:
        print('You must to specify one or more jobs to get_op_metrics', file=stderr)
        return False
    logger.info("epmt_list_op_metrics: %s", str(arglist))
    kwargs = kwargify(arglist)
    ops = get_op_metrics(**kwargs)
    if not isinstance(ops, DataFrame) or len(ops) == 0:
        logger.info("get_op_metrics %s returned no op metrics", str(kwargs))
        return False
    print(ops)
    return True


def epmt_list_refmodels(arglist):
    '''
    List reference models stored in the database.

    Parses arglist as keyword arguments and passes them to get_refmodels.
    Returns False if no reference models are found, True otherwise.
    '''
    logger.info("epmt_list_refmodels: %s", str(arglist))
    kwargs = kwargify(arglist)
    jobs = get_refmodels(**kwargs)
    if len(jobs) == 0:
        logger.info("get_refmodels %s return no refmodels", str(kwargs))
        return False
    print(jobs)
    return True


def epmt_list_job_proc_tags(arglist):
    '''
    List job/process tag associations from the database.

    Parses arglist as keyword arguments and passes them to get_job_proc_tags.
    Returns False if no tags are found, True otherwise.
    '''
    logger.info("epmt_list_job_proc_tags: %s", str(arglist))
    kwargs = kwargify(arglist)
    jobs = get_job_proc_tags(**kwargs)
    if len(jobs) == 0:
        logger.info("get_job_proc_tags %s returned no tags", str(kwargs))
        return False
    print(jobs)
    return True
