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
  unprocessed_jobs=$(python3 -c "from epmt import epmt_query as eq; print(eq.get_unprocessed_jobs())")
  logfile=$(epmt -h | grep logfile|cut -f2 -d:)
}


@test "no daemon running" {
  [[ "$unprocessed_jobs" == "[]" ]]
  assert_success
  run epmt daemon
  assert_output --partial "EPMT daemon not running"
}


@test "start epmt daemon" {
  [[ "$unprocessed_jobs" == "[]" ]]
  assert_success
  trap sig_handler SIGINT SIGTERM SIGQUIT SIGHUP
  run epmt -v daemon --start
  run epmt daemon
  assert_output --partial "EPMT daemon running PID"
  sleep 1
  run grep "starting daemon loop" $logfile
  assert_success
}


@test "stop epmt daemon" {
  [[ "$unprocessed_jobs" == "[]" ]]
  assert_success
  run epmt daemon
  assert_output --partial "EPMT daemon running PID"
  run epmt daemon --stop
  assert_output --partial "Sending signal to EPMT daemon pid"
  # check on logfile and cleanup up as we did verbose logging to log file
  ls $logfile
  assert_success
  rm -f $logfile
}
