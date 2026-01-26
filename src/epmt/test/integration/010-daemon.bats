load 'libs/bats-support/load'
load 'libs/bats-assert/load'

function sig_handler() {
  echo "Cleaning up daemons.."
  # we run in a loop for a few seconds, as sometimes
  # epmt daemon --start takes a couple of seconds before
  # it has the lockfile in place
  for i in 1 2 3 4 5; do
    epmt daemon --stop > /dev/null 2>&1 && return
    sleep 1
  done
}

setup() {
#  WHICH_EPMT=$(command -v epmt)
#  echo "epmt is ${WHICH_EPMT}"
#  EPMT_DIRNAME=$(dirname $WHICH_EPMT)
#  echo "epmt dir is ${EPMT_DIRNAME}"
#  PYTHON=$EPMT_DIRNAME/python
#  ls $PYTHON && echo "python is there" || echo "python not there"
#  echo "python is ${PYTHON}"
#  $PYTHON --version
  unprocessed_jobs=$(python3 -c "from epmt import epmt_query as eq; print(eq.get_unprocessed_jobs())")
# Assuming this from the settings provided with the tests, this sucks
  logfile=$(epmt -h | grep logfile|cut -f2 -d:)
}


@test "no daemon running" {
  ## inl: why is this skip here?
  [[ "$unprocessed_jobs" == "[]" ]] || skip "there are unprocessed jobs in database"
  run epmt daemon
  assert_output --partial "EPMT daemon not running"
}

@test "start epmt daemon" {
  ## inl: why is this skip here?
  [[ "$unprocessed_jobs" == "[]" ]] || skip "there are unprocessed jobs in database"
  trap sig_handler SIGINT SIGTERM SIGQUIT SIGHUP
  run epmt -v daemon --start
  run epmt daemon
  assert_output --partial "EPMT daemon running PID"
  sleep 1
  run grep "starting daemon loop" $logfile
#  echo $logfile
#  ls -al $logfile
#  cat $logfile
#  ls -Art /tmp/epmt_*[0-9].log | tail -n 1)
  assert_success
}


@test "stop epmt daemon" {
  ## inl: why is this skip here?
  [[ "$unprocessed_jobs" == "[]" ]] || skip "there are unprocessed jobs in database"
  run epmt daemon
  assert_output --partial "EPMT daemon running PID"
  run epmt daemon --stop
  assert_output --partial "Sending signal to EPMT daemon pid"
  # check on logfile and cleanup up as we did verbose logging to log file
  ls $logfile
  assert_success
  rm -f $logfile
}
