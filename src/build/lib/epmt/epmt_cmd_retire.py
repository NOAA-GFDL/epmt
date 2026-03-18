"""
EPMT retire command module - handles retirement of jobs and models.
"""
from logging import getLogger

import tracemalloc as tm

from epmt.epmt_query import retire_jobs, retire_refmodels
from epmt.epmt_settings import retire_models_ndays, retire_jobs_ndays

logger = getLogger(__name__)

def epmt_retire(skip_unprocessed=False, dry_run=False):
    '''
    remove jobs from the database that are older than retirement threshold. can skip jobs that have yet to be 
    postprocessed (and thus, are likely to have not been analysed yet). can also dry run and inform the user
    of how many jobs and models the routine will likely delete
    '''

    tm.start()

    logger.warning('Retiring models older than %d days', retire_models_ndays)
    num_models_retired = retire_refmodels(retire_models_ndays, dry_run=dry_run)


    model_retire_size, model_retire_peak = tm.get_traced_memory()
    model_retire_memsize_mib = model_retire_size/1024/1000
    model_retire_mempeak_mib = model_retire_peak/1024/1000
    logger.info( 'after model retire: memory_size=%d MiB, memory_peak=%d MiB', model_retire_memsize_mib,
                                                                               model_retire_mempeak_mib )
    tm.reset_peak()


    logger.warning('Retiring jobs older than %d days', retire_jobs_ndays)
    num_jobs_retired = retire_jobs(retire_jobs_ndays, skip_unprocessed=skip_unprocessed, dry_run=dry_run)


    job_retire_size, job_retire_peak = tm.get_traced_memory()
    job_retire_memsize_mib=job_retire_size/1024/1000
    job_retire_mempeak_mib=job_retire_peak/1024/1000
    logger.info( 'after job retire: memory_size=%d MiB, memory_peak=%d MiB', job_retire_memsize_mib,
                                                                             job_retire_mempeak_mib )
    tm.reset_peak()


    logger.info('%d jobs retired, %d models retired', num_jobs_retired, num_models_retired)
    if dry_run:
        logger.info(f'(dry_run=True) {num_jobs_retired} jobs and {num_models_retired} models will be retired')

    # end memory tracing
    tm.stop()

    return (num_jobs_retired, num_models_retired)
