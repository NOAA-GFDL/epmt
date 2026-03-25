import pytest

from epmt import epmt_query as eq
from epmt.orm import ReferenceModel


def test_save_refmodel_create_and_cleanup():
    # need at least 3 jobs to create a meaningful model; skip otherwise
    jobs = eq.get_jobs(limit=3, fmt='terse', trigger_post_process=False)
    if not jobs or len(jobs) < 3:
        pytest.skip("Not enough jobs available to create a reference model")

    jobs_sample = jobs[:3]
    computed = {'pytest_dummy': {'duration': [0.0, [0.0]]}}
    info_dict = {'test': True}

    # create the model (use tag kwarg as some callers use `tag`)
    r = eq.save_refmodel(ReferenceModel, jobs=jobs_sample, computed=computed, info_dict=info_dict, enabled=True, name='pytest_save_refmodel', tag={}, op_tags=[], fmt='dict')
    assert isinstance(r, dict)
    assert 'id' in r
    ref_id = int(r['id'])

    try:
        # verify it exists
        models = eq.get_refmodels(fmt='terse')
        assert ref_id in models
    finally:
        # ensure cleanup: delete the created model
        deleted = eq.delete_refmodels(ref_id)
        # delete_refmodels returns number deleted or 0
        assert deleted >= 0
        models_after = eq.get_refmodels(fmt='terse')
        assert ref_id not in models_after
        # run retire_refmodels (dry run) to exercise cleanup paths
        eq.retire_refmodels(ndays=1, dry_run=True)
