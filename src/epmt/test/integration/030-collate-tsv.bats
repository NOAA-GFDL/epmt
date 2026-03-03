load 'libs/bats-support/load'
load 'libs/bats-assert/load'

# Use a file-based SQLite database for persistence across epmt commands
export EPMT_DB_URL="sqlite:////tmp/epmt_test_collate_tsv.sqlite"

setup(){
  stage_dest=$(epmt -h | sed -n 's/stage_command_dest://p')
  test -n "${stage_dest}" || fail
  test -d ${stage_dest} || fail
  jobs_in_module='989'
  # Clean up any previous test state
  rm -f /tmp/epmt_test_collate_tsv.sqlite
  rm -f ${stage_dest}/989.tgz
  epmt delete ${jobs_in_module} 2>/dev/null || true

}
teardown() {
  stage_dest=$(epmt -h | sed -n 's/stage_command_dest://p')
  jobs_in_module='989'
  epmt delete ${jobs_in_module} 2>/dev/null || true
  rm -f ${stage_dest}/989.tgz
  rm -f /tmp/epmt_test_collate_tsv.sqlite
}

@test "epmt with COLLATED_TSV" {

  jobid=989
  export SLURM_JOB_ID=$jobid
  export EPMT_JOB_TAGS='op:check-tsv'
  epmt start           # Generate prolog
  # set up environment while forcing PAPIEX_OPTIONS to include COLLATED_TSV
  eval `epmt source| sed '/^PAPIEX_OPTIONS/ s/PAPIEX_OPTIONS=/PAPIEX_OPTIONS=COLLATED_TSV,/'`
  /bin/sleep 1 2>/dev/null >&2 # Workload
  epmt_uninstrument    # End Workload, disable instrumentation
  epmt stop            # Wrap up job stats
  f=`epmt stage`       # Move to medium term storage ($PWD)
  epmt -v submit $f       # Submit to DB
  epmt list | grep -w $jobid > /dev/null

  # Unfortunately, the test below won't work with the CI pipeline
  # as we have no processes to import. So our check whether
  # get_procs triggers processing is doomed to fail on a CI system
  # where papiex won't be there.
  #
  # lets see if we have fixed the bug wherein calling
  # get_procs prior to post-processing a job would return no
  # processes
  # run epmt list procs jobs=$jobid limit=1
  # assert_success

  run epmt dump -k tags $jobid
  assert_output --partial "{'op': 'check-tsv'}"
  run test -f ${stage_dest}/989.tgz
  assert_success
  rm -f ${stage_dest}/989.tgz
}
