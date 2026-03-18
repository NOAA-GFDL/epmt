"""
Mock outlier methods
"""

from logging import getLogger, basicConfig, DEBUG, ERROR, INFO, WARNING  # pylint: disable=unused-import
logger = getLogger(__name__)  # pylint: disable=invalid-name
basicConfig(level=DEBUG)


def detect_outlier_jobs(jobs, trained_model=None, features=['cpu_time', 'duration', 'num_procs'], methods=[
                        'modified_z_score'], thresholds='thresholds', sanity_check=True):
    """
    (df, parts) = eod.detect_outlier_jobs(jobs)
    pprint(parts)
    {'cpu_time': ([u'kern-6656-20190614-190245',
                   u'kern-6656-20190614-194024',
                   u'kern-6656-20190614-191138'],
                  [u'kern-6656-20190614-192044-outlier']),
     'duration': ([u'kern-6656-20190614-190245',
                   u'kern-6656-20190614-194024',
                   u'kern-6656-20190614-191138'],
                  [u'kern-6656-20190614-192044-outlier']),
     'num_procs': ([u'kern-6656-20190614-190245',
                    u'kern-6656-20190614-192044-outlier',
                    u'kern-6656-20190614-194024',
                    u'kern-6656-20190614-191138'],
                   [])}
    """
    returns = ('df', {'feature': (['job', 'job2'], ['joboutlier'])})
    return "Running outlier analysis on Jobs: " + str(jobs)
