#!/usr/bin/env python
"""
Utility to go through the kronos run folder, and generate a KPF file from the allinea jsons..
"""
import os
# import shutil
import subprocess
import json
import argparse

from exceptions_iows import ConfigurationError
from kronos_io.profile_format import ProfileFormat
from logreader import profiler_reader

if __name__ == "__main__":

    # Parser for the required arguments
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path_run", type=str, help="Path of the kronos run folder (contains the job-<ID> sub-folders)")
    parser.add_argument("kpf_file", type=str, help="Name of the KPF file to write out")
    parser.add_argument("--m2j", type=str, help="Name of the map2json file used for conversion")
    args = parser.parse_args()

    if not os.path.exists(args.path_run):
        raise ConfigurationError("Specified run path does not exist: {}".format(args.path_run))

    if args.m2j:
        local_map2json_file = args.m2j
    else:
        local_map2json_file = os.path.join(os.getcwd(), "map2json.py")

    # path of the output kpf file
    path_kpf = os.path.dirname(os.path.abspath(args.kpf_file))
    if not os.path.exists(path_kpf):
        os.mkdir(path_kpf)

    # create a temporary folder where to store all the converted map files..
    tmp_dir_abs = os.path.join(path_kpf, 'tmp')
    if not os.path.exists(tmp_dir_abs):
        os.mkdir(tmp_dir_abs)

    print "writing output kpf file: {}".format(args.kpf_file)

    # ---------- Process the run jsons ----------
    job_dirs = [x for x in os.listdir(args.path_run) if os.path.isdir(os.path.join(args.path_run, x))]

    fname_list = []
    dict_name_label = {}
    for job_dir in job_dirs:

        sub_dir_path_abs = os.path.join(args.path_run, job_dir)
        sub_dir_files = os.listdir(sub_dir_path_abs)
        map_file = [f for f in sub_dir_files if f.endswith('.map')]

        if map_file:

            map_file_name = map_file[0]
            map_file_full = os.path.join(sub_dir_path_abs, map_file_name)
            map_base_abs = os.path.splitext(map_file_full)[0]
            print "processing MAP file: {}".format(map_file_full)

            json_file_name = map_base_abs + '.json'
            input_file_path_abs = os.path.join(sub_dir_path_abs, 'input.json')

            # read the corresponding json of the input and read the label
            with open(input_file_path_abs, 'r') as f:
                json_data = json.load(f)

            label = json_data['metadata']['workload_name']
            job_ID = json_data['job_num']

            # copy all the maps and the converted jsons into the tmp folder..
            allinea_json = 'allinea_job-'+str(job_ID)+'.json'
            allinea_map = 'allinea_job-'+str(job_ID)+'.map'

            subprocess.Popen(["python", local_map2json_file,
                              os.path.join(sub_dir_path_abs, map_file_name)]).wait()

            subprocess.Popen(['cp',
                              os.path.join(sub_dir_path_abs, json_file_name),
                              os.path.join(tmp_dir_abs, allinea_json)]).wait()

            subprocess.Popen(['cp',
                              os.path.join(sub_dir_path_abs, map_file_name),
                              os.path.join(tmp_dir_abs, allinea_map)]).wait()

            fname_list.append(allinea_json)
            dict_name_label[allinea_json] = label

    fname_list.sort()

    job_map_dataset = profiler_reader.ingest_allinea_profiles(tmp_dir_abs,
                                                              list_json_files=fname_list,
                                                              json_label_map=dict_name_label)

    pf = ProfileFormat(model_jobs=job_map_dataset.model_jobs(), workload_tag='allinea_map_files')

    # write output kpf file..
    with open(args.kpf_file, 'w') as f:
        pf.write(f)

    # remove tmp folder
    # shutil.rmtree(tmp_dir_abs)

