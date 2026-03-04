#!/bin/bash
# Helper script for escape character integration test.
# This is run as an external script to avoid Python/bash quoting issues.
export SLURM_JOB_ID=12340
export SLURM_JOB_NAME=12340_name
epmt start
eval `epmt source`
# begin workload
cut -d\" -f2 < /dev/null
/bin/echo '\\\'
/bin/echo \ b
/bin/echo \\
/bin/echo ,
/bin/echo \'
/bin/echo -e "\tHello"
/bin/echo -e "\tThereU\nR"
/bin/echo -e \\\a
/bin/echo -e "\a"
/bin/echo -e \\
/bin/echo -e 'some test \b and more text'
/bin/echo \b
/bin/echo \\b
/bin/echo '\b'
/bin/echo -e '\. some text'
/bin/echo -e 'try\.some more text'
sed 's/^\.//' < /dev/null
# end workload
epmt_uninstrument
epmt stop
epmt submit --remove
