load 'libs/bats-support/load'
load 'libs/bats-assert/load'

# This test requires a persistent (file-based) sqlite DB since data must persist
# across multiple epmt command invocations (submit, dump, explore).
# The default in-memory sqlite DB creates a new DB per process invocation.
export EPMT_DB_URL="sqlite:////tmp/epmt_explore_test.db"

setup() {
  resource_path="${PWD}/src/epmt"
  test -n "${resource_path}" || fail
  test -d ${resource_path} || fail

  # Clean up any existing test DB to start fresh
  rm -f /tmp/epmt_explore_test.db

  jobs_in_module='625151 627907 629322 633114 675992 680163 685000 685001 685003 685016 691209 692500 693129'

  # Delete pre-existing jobs if any (expected to fail on fresh DB, so guard with || true)
  epmt delete ${jobs_in_module} 2>/dev/null || true

  # Submit test data - must succeed for test to be meaningful
  run epmt submit ${resource_path}/test/data/submit/692500.tgz ${resource_path}/test/data/query/*.tgz ${resource_path}/test/data/outliers_nb/{625151,627907,629322,633114,675992,680163,685001,691209,693129}.tgz
  if [ "$status" -ne 0 ]; then
    echo "Setup failed: epmt submit returned status $status"
    echo "Output: $output"
    return 1
  fi

  # Force post-processing - must succeed
  run epmt dump ${jobs_in_module}
  if [ "$status" -ne 0 ]; then
    echo "Setup failed: epmt dump returned status $status"
    echo "Output: $output"
    return 1
  fi
}

teardown() {
  # Clean up jobs (may not exist if setup failed, so guard with || true)
  epmt delete ${jobs_in_module} 2>/dev/null || true
  rm -f /tmp/epmt_explore_test.db
}

@test "epmt explore (can take a couple of minutes)" {
  run epmt explore ESM4_historical_D151
  # assert_output --partial "Experiment ESM4_historical_D151 contains 13 jobs: 625151,627907,629322,633114,675992,680163,685000..685001,685003,685016,691209,692500,693129"
  assert_output --partial "ocean_annual_z_1     18540101       625151      10425623185   ****"
  assert_output --partial "ocean_annual_z_1     18590101       627907       6589174875"
  assert_output --partial "ocean_annual_z_1     18890101       691209        860163243   ****"
  assert_output --partial "ocean_annual_z_1     18940101       693129       3619324767     **"
  assert_output --partial "18540101      10425623185"
  assert_output --partial "18840101      26897098077   ****"
}
