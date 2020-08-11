#!/bin/bash
{{ scheduler_params }}

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
    {{ notify_script }} --type="NotifyMetadata" stepper "{\"step\": $step}"
done

# send end-of-job msg
{{ notify_script }} --type="Complete" stepper
