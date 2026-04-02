"""
EPMT show command module - handles job display functionality.
"""

from logging import getLogger

import epmt.epmt_query as eq

logger = getLogger(__name__)

def epmt_show_job(jobid, key=None):
    if isinstance(jobid, list):
        jobid = jobid[0]
    jobs = eq.get_jobs([jobid], fmt='dict')
    if len(jobs) != 1:
        logger.error('Job %s could not be found in database', jobid)
        return False
    j_dict = jobs[0]
    if key:
        if key in j_dict:
            print(j_dict[key])
        else:
            logger.error('Key "%s" was not found as an attribute of the job table', key)
            print(f'Here are the keys that were found: {",".join(sorted(j_dict.keys()))}')
            return False
    else:
        for k in sorted(j_dict.keys()):
            print(f"{k:<20}      {j_dict[k]!s:<20}")
    return True
