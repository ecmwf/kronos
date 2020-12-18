#!/bin/bash
{{ scheduler_params }}

export KRONOS_WRITE_DIR="{{ write_dir }}"
export KRONOS_READ_DIR="{{ read_dir }}"
export KRONOS_SHARED_DIR="{{ shared_dir }}"
export KRONOS_TOKEN="{{ simulation_token }}"

{{ env_setup }}

cd {{ job_dir }}

{%- for job in jobs %}
{{ job.submit_script }} >{{ job.output_file }} 2>{{ job.error_file }}
{% endfor %}

{{ kronos_notify }} --type="Complete" composite
