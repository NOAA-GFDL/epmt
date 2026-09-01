"""
Integration tests for epmt annotate functionality.

Translated from 015-annotate.bats.
"""
import os
import pytest

from conftest import run_cmd, epmt_setting, epmt_python_setting

# Use a file-based SQLite database for persistence across epmt commands
EPMT_DB_PATH = '/tmp/epmt_test_annotate.sqlite'
EPMT_DB_URL = f"sqlite:///{EPMT_DB_PATH}"


@pytest.fixture(autouse=True)
def setup_and_teardown(resource_path):
    """
    run annotate script; yield relevant fields, then tear-down/clean-up.
    """
    stage_dest = epmt_setting("stage_command_dest")
    assert stage_dest, "stage_command_dest is empty"
    assert os.path.isdir(stage_dest), f"stage_command_dest {stage_dest} does not exist"

    epmt_output_prefix = epmt_python_setting(
        "import epmt.epmt_settings as settings; print(settings.epmt_output_prefix);"
    )
    assert epmt_output_prefix, "epmt_output_prefix is empty"

    user = os.environ.get("USER", "root")
    env = {"EPMT_DB_URL": EPMT_DB_URL}

    # Clean up any previous test state
    for f in [EPMT_DB_PATH]:
        if os.path.exists(f):
            os.remove(f)

    job_dir = os.path.join(epmt_output_prefix, user, "3456")
    if os.path.isdir(job_dir):
        import shutil
        shutil.rmtree(job_dir)

    staged_file = os.path.join(stage_dest, "3456.tgz")
    if os.path.exists(staged_file):
        os.remove(staged_file)

    run_cmd("epmt delete 3456", env=env)

    annotate_out = run_cmd(f"{resource_path}/test/integration/epmt-annotate.sh", env=env)
    assert annotate_out.returncode == 0, f'epmt-annotate.sh failed:\n{annotate_out.stderr}\n{annotate_out.stdout}'
    assert os.path.exists(EPMT_DB_PATH), f'does not exist: EPMT_DB_PATH={EPMT_DB_PATH}'
    assert os.path.exists(f'{stage_dest}/3456.tgz'), f'{stage_dest}/3456.tgz was not created for some reason'

    yield {"stage_dest": stage_dest, "env": env}

    # Teardown
    if os.path.isdir(job_dir):
        import shutil
        shutil.rmtree(job_dir)
    if os.path.exists(staged_file):
        os.remove(staged_file)
    if os.path.exists(EPMT_DB_PATH):
        os.remove(EPMT_DB_PATH)

    run_cmd("epmt delete 3456", env=env)


class TestAnnotate:
    def test_epmt_annotate_read_tgz(self, setup_and_teardown):
        ctx = setup_and_teardown
        stage_dest = ctx["stage_dest"]
        env = ctx["env"]
        r = run_cmd(f"epmt dump -k annotations {stage_dest}/3456.tgz", env=env)
        assert r.returncode == 0, f"epmt dump failed: {r.stderr}"
        output = r.stdout
        assert "'inbetween_1': 1, 'inbetween_2': 1" in output
        assert "'c': 200, 'd': 400, 'e': 300, 'f': 600" in output

    def test_epmt_annotate_write_db(self, setup_and_teardown):
        ctx = setup_and_teardown
        env = ctx["env"]
        run_cmd("epmt annotate 3456 g=400 h=800", env=env)
        r = run_cmd("epmt dump -k annotations 3456", env=env)
        assert "'c': 200, 'd': 400, 'e': 300, 'f': 600" in r.stdout
        assert "'g': 400, 'h': 800" in r.stdout
        # Set EPMT_JOB_TAGS
        run_cmd(
            "epmt annotate 3456 EPMT_JOB_TAGS='exp_name:abc;exp_component:def;exp_time:18540101'",
            env=env,
        )
        r = run_cmd("epmt dump -k tags 3456", env=env)
        assert "'exp_name': 'abc'" in r.stdout
        assert "'exp_component': 'def'" in r.stdout
        assert "'exp_time': '18540101'" in r.stdout

    def test_epmt_annotate_replace(self, setup_and_teardown):
        ctx = setup_and_teardown
        env = ctx["env"]
        r = run_cmd(
            'epmt annotate --replace 3456 a=100 EPMT_JOB_TAGS="jobid:3456"', env=env
        )
        assert r.returncode == 0
        r = run_cmd("epmt dump -k tags 3456", env=env)
        assert r.returncode == 0
        assert "{'jobid': '3456'}" in r.stdout
        # Replace with new values
        r = run_cmd("epmt annotate --replace 3456 a=200", env=env)
        assert r.returncode == 0
        r = run_cmd("epmt dump -k annotations 3456", env=env)
        assert r.returncode == 0
        assert "{'a': 200}" in r.stdout

    def test_epmt_annotate_replace_jobtags(self, setup_and_teardown):
        ctx = setup_and_teardown
        env = ctx["env"]
        r = run_cmd(
            'epmt annotate --replace 3456 a=100 EPMT_JOB_TAGS="jobid:3456;ocn_res:0.5l75"',
            env=env,
        )
        assert r.returncode == 0
        r = run_cmd("epmt dump -k tags 3456", env=env)
        assert r.returncode == 0
        assert "'jobid': '3456'" in r.stdout
        assert "'ocn_res': '0.5l75'" in r.stdout
        # Replace tags
        r = run_cmd(
            "epmt annotate --replace 3456 'EPMT_JOB_TAGS'='jobid:123'", env=env
        )
        assert r.returncode == 0
        r = run_cmd("epmt dump -k tags 3456", env=env)
        assert r.returncode == 0
        assert "{'jobid': '123'}" in r.stdout

    def test_epmt_bad_annotate(self, setup_and_teardown):
        ctx = setup_and_teardown
        env = ctx["env"]
        r = run_cmd("epmt annotate abc", env=env)
        assert r.returncode != 0
        assert "No annotations found" in r.stdout + r.stderr
        r = run_cmd("epmt annotate 3456 abc", env=env)
        assert r.returncode != 0
        assert "Annotations must be of the form <key>=<value>" in r.stdout + r.stderr

    def test_epmt_annotate_incomplete(self, setup_and_teardown):
        ctx = setup_and_teardown
        env = ctx["env"]
        # Set known annotation state
        r = run_cmd(
            'epmt annotate --replace 3456 a=100 EPMT_JOB_TAGS="jobid:3456"', env=env
        )
        assert r.returncode == 0
        r = run_cmd("epmt dump -k annotations 3456", env=env)
        assert r.returncode == 0
        assert "'a': 100" in r.stdout
        assert "'EPMT_JOB_TAGS': 'jobid:3456'" in r.stdout
        # Incomplete annotation
        r = run_cmd("epmt annotate --replace 3456 'test'=", env=env)
        assert r.returncode != 0
        # Verify annotations unchanged
        r = run_cmd("epmt dump -k annotations 3456", env=env)
        assert r.returncode == 0
        assert "'a': 100" in r.stdout
        assert "'EPMT_JOB_TAGS': 'jobid:3456'" in r.stdout

    def test_epmt_annotate_tag_incomplete(self, setup_and_teardown):
        ctx = setup_and_teardown
        env = ctx["env"]
        r = run_cmd(
            'epmt annotate --replace 3456 a=100 EPMT_JOB_TAGS="jobid:3456"', env=env
        )
        assert r.returncode == 0
        r = run_cmd("epmt dump -k tags 3456", env=env)
        assert r.returncode == 0
        assert "{'jobid': '3456'}" in r.stdout
        # Incomplete tag
        r = run_cmd("epmt annotate 3456 'EPMT_JOB_TAGS'=", env=env)
        assert r.returncode != 0
        r = run_cmd("epmt dump -k tags 3456", env=env)
        assert r.returncode == 0
        assert "{'jobid': '3456'}" in r.stdout

    def test_epmt_annotate_backslash(self, setup_and_teardown):
        ctx = setup_and_teardown
        env = ctx["env"]
        r = run_cmd(
            'epmt annotate --replace 3456 a=100 EPMT_JOB_TAGS="jobid:3456"', env=env
        )
        assert r.returncode == 0
        r = run_cmd("epmt dump -k tags 3456", env=env)
        assert r.returncode == 0
        assert "{'jobid': '3456'}" in r.stdout
        # Tags with backslash
        r = run_cmd(
            r"epmt annotate --replace 3456 'EPMT_JOB_TAGS'='\test:\hello'", env=env
        )
        assert r.returncode == 0
        r = run_cmd("epmt dump -k tags 3456", env=env)
        assert r.returncode == 0
        # Note: backslash handling may vary
        assert "test" in r.stdout
        assert "hello" in r.stdout
