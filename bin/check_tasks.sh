#!/bin/bash

set -e 

kronos_commands=(
"./kronos-model"
"./kronos-enquire-global-config"
"./kronos-generate-read-files"
"./kronos-executor"
"./kronos-format-config-exe"
"./kronos-krf-2-kpf"
"./kronos-show-info"
"./kronos-show-job"
"./kronos-plot-kpf"
"./kronos-view-json"
"./kronos-raw-to-kpf"
"./kronos-format-config-export"
"./kronos-format-config-model"
"./kronos-inspect-dataset"
"./kronos-check-results"
"./kronos-analyse-results"
"./kronos-collect-results"
"./kronos-format-kpf"
"./kronos-summarise-results"
"./kronos-format-krf"
"./kronos-format-ksf"
"./kronos-generate-dummy-workload"
"./kronos-ingest-logs"
)

for i in ${kronos_commands[*]}; do

  # try executing the command without options
   echo "-------> Executing command: $i with no arguments and checking that return code == 0"
   no_argument_command=`$i`
   if [ $? -eq 0 ]; then
       echo Command $i executed with no argument exited with 0 status! WRONG..
       exit 1 
   else
       echo OK!
   fi

  # try executing the command with --help option
   echo "-------> Executing command: $i --help"
   help_command=$($i --help)
   commands_status=$?
   if [ $commands_status -ne 0 ]; then
       echo command $i executed with --help exited with $commands_status status! WRONG..
       exit 1 
   else
       echo command $i executed with --help exited with 0 status! OK!
   fi

   if [ "$help_command" == "$no_argument_command" ]; then
       echo command $i executed with --help is equal to command executed without help.. OK!
   else
       echo ERROR: command $i executed with --help is NOT equal to command executed without help..
       echo "------------------------"
       echo $no_argument_command 
       echo "------------------------"
       echo $help_command
       echo "------------------------"
       exit 1
   fi
        
done
