'''
a quick postprocessing script targeting only unprocessed jobs as returned by the raw sql query below
'''

#import ast
import logging
from logging import INFO #DEBUG, INFO, WARNING, ERROR

#import epmt
from epmt.epmt_cmd_retire import epmt_retire
from epmt import epmt_query as eq
from epmt.orm.sqlalchemy import orm_raw_sql

logger = logging.getLogger(__name__)
logger.setLevel(INFO)


def epmt_dbcare():
    job_list=[]

    ## RETIRE JOBS
    logger.info('retiring jobs first.')
    epmt_retire(skip_unprocessed=True,dry_run=False)

    ## VACUUM DB TABLES ( TEST THIS APPROACH BY HAND FIRST )
    psql_set_max_parallel_workers='SET max_parallel_maintenance_workers = 0;'
    psql_stub_vacuum='VACUUM VERBOSE '
    psql_vacuum_jobs=psql_stub_vacuum + 'jobs;'
    psql_vacuum_procs_stag=psql_stub_vacuum + 'processes_staging;'
    psql_vacuum_procs=psql_stub_vacuum + 'processes;'

    logger.info('vacuuming jobs table of dead rows')
    result_vacuum_jobs=orm_raw_sql(psql_vacuum_jobs)
    logger.info('result: %s', str(result_vacuum_jobs.scalars().all() ) )

    logger.info('vacuuming processes_staging table of dead rows')
    result_vacuum_procs_stag=orm_raw_sql(psql_vacuum_procs_stg)
    logger.info('result: %s', str(result_vacuum_procs_stag.scalars().all() ) )

    logger.info('vacuuming processes table of dead rows')
    result_vacuum_procs=orm_raw_sql(psql_vacuum_procs)
    logger.info('result: %s', str(result_vacuum_procs.scalars().all() ) )

    ## POST PROCESS JOBS
    # postgreSQL statement(s) to count number of unprocessed jobs and retrieve job ids as a list of str
    psql_stub_get_unprocd_jobs="select jobid from jobs where (info_dict -> 'procs_in_process_table')::int = 0"
    psql_get_unprocd_jobs=psql_stub_get_unprocd_jobs + ';'
    psql_count_unprocd_jobs="select COUNT(*) from ( " + psql_stub_get_unprocd_jobs + " ) as my_results;"


    # get count of unprocd jobs and see if there are any to postprocess
    result_count_unprocd_jobs=orm_raw_sql(psql_count_unprocd_jobs)
    number_of_unprocd_jobs=-1
    try:
        number_of_unprocd_jobs=result_count_unprocd_jobs.scalars().all()[0]
    except Exception as e:
        raise Exception from e

    # check the count of unprocd jobs
    if number_of_unprocd_jobs == 0:
        logger.info('number of unprocessed jobs is 0, nothing to do. exit.')
        raise ValueError('number of unprocessed jobs is 0, nothing to do. exit.')
    if number_of_unprocd_jobs < 0:
        raise ValueError('could not initialize number of unprocd jobs. should not happen. examine code and DB state.')

    logger.info('%s unprocessed jobs found, postprocessing them', number_of_unprocd_jobs)


    # stuff to do! get the jobid list
    result_get_unprocd_jobs=orm_raw_sql(psql_get_unprocd_jobs)
    try:
        logger.info('attempting to retrieve job IDs')
        job_list=result_get_unprocd_jobs.scalars().all()
        logger.debug('job_list is %s', ' '.join(job_list))
    except Exception as e:
        raise Exception('problem with retrieving job IDs. inspect code.') from e

    # postprocess the unprocessed jobs
    num_jobs_ppd=eq.post_process_jobs(jobs=job_list)
    if num_jobs_ppd > 0:
        logger.info('success, num_jobs_ppd = %s', num_jobs_ppd )
    else:
        logger.warning('problem, num_jobs_ppd not greater than 0.')
        logger.warning('num_jobs_ppd = %s', num_jobs_ppd )
