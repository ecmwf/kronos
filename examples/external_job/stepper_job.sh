#!/bin/bash
{{ scheduler_params }}

handle_error() {
  set +e
  trap 0
  {{ kronos_notify }} --type="Failed" stepper
  exit 1
}
trap handle_error ERR

set -e

wdir="{{ write_dir }}"
cd $wdir

# kronos simulation token
export KRONOS_TOKEN="{{ simulation_token }}"

{{ source_kronos_env }}

nsteps={{ nsteps }}

for step in $(seq 1 $nsteps) ; do
    echo "Running step $step"
    {{ stepper }} $step {{ size_kb }} "{{ shared_dir }}"
    sleep 10
    {{ kronos_notify }} --type="NotifyMetadata" stepper "{\"step\": $step}"
done

# send end-of-job msg
{{ kronos_notify }} --type="Complete" stepper
