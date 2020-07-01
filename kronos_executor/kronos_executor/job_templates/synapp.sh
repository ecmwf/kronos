#!/bin/bash
{{ scheduler_params }}

export KRONOS_WRITE_DIR="{{ write_dir }}"
export KRONOS_READ_DIR="{{ read_dir }}"
export KRONOS_SHARED_DIR="{{ shared_dir }}"
export KRONOS_TOKEN="{{ simulation_token }}"

{{ env_setup }}

{{ profiling_code }}

# Change to the original directory for submission
cd {{ job_dir }}

{{ launch_command }} {{ coordinator_binary }} {{ input_file }}
