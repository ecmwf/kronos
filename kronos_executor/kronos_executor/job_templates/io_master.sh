#!/bin/bash
{{ scheduler_params }}

export KRONOS_WRITE_DIR="{{ write_dir }}"
export KRONOS_READ_DIR="{{ read_dir }}"
export KRONOS_SHARED_DIR="{{ shared_dir }}"
export KRONOS_TOKEN="{{ simulation_token }}"

{{ env_setup }}

# Call the io master and configure it with the appropriate I/O tasks in the time_schedule
{{ kronos_bin_dir }}/remote_io_master {{ ioserver_hosts_file }} {{ input_file }}

sleep 5

{{ send_complete_msg }}

