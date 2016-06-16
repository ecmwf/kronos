from real_job import RealJob


def read_pbs_logs(filename_in):

    pbs_jobs = []

    log_list_raw = ['ctime',
                    'qtime',
                    'etime',
                    'end',
                    'start',
                    'resources_used.ncpus',
                    'Resource_List.ncpus',
                    'resources_used.mem',
                    'resources_used.cpupercent',
                    'resources_used.cput',
                    'group',
                    'jobname',
                    ]

    # - standardize the key nemes for different PBS log labels..
    log_list = ['time_created',
                'time_queued',
                'time_eligible',
                'time_end',
                'time_start',
                'ncpus',
                'ncpus',
                'memory_kb',
                'cpu_percent',
                'cpu_percent',
                'group',
                'jobname',
                ]

    # Read file
    f_in = open(filename_in, "r")
    cc = 1
    cce = 0
    for line in f_in:

        # array got by splitting the line
        larray = line.split(" ")
        b1_array = larray[1].split(";")

        # block E
        if (b1_array[1] == "E"):

            # init dictionary
            line_dict = {}

            # user name
            user_name = b1_array[3].split("=")[1]
            line_dict["user"] = str(user_name)

            for jobL in range(0, len(larray)):

                yval_val = larray[jobL].split("=")
                # print yval_val

                if (len(yval_val) == 2):
                    if yval_val[0] in log_list_raw:
                        # find index
                        idx = log_list_raw.index(yval_val[0])
                        key_name = log_list[idx]
                        line_dict[key_name] = yval_val[1].strip()

                # special case for ARCTUR PBS..
                # Resource_List.nodes=1:ppn=1
                if yval_val[0] == "Resource_List.nodes":
                    if len(yval_val[1].split(":")) > 1:
                        if yval_val[1].split(":")[1] == "ppn":
                            line_dict["ncpus"] = int(
                                yval_val[2]) * int(yval_val[1].split(":")[0])
                    else:
                        line_dict["ncpus"] = int(yval_val[1])

            i_job = RealJob()

            # print 'i_job.time_created ', line_dict['time_created']
            # print 'i_job.time_queued  ', line_dict['time_queued']
            # print 'i_job.time_eligible', line_dict['time_eligible']
            # print 'i_job.time_end     ', line_dict['time_end']
            # print 'i_job.time_start   ', line_dict['time_start']
            # print int(line_dict['ncpus'])
            # print line_dict['time_created']
            # print type( line_dict['time_created'] )
            # print any(c.isalpha() for c in line_dict['time_created'])

            # created  time
            if any([c.isalpha() for c in line_dict['time_created']]):
                i_job.time_created = -1
            else:
                i_job.time_created = int(line_dict['time_created'])

            # queue time
            if any([c.isalpha() for c in line_dict['time_queued']]):
                i_job.time_queued = -1
            else:
                i_job.time_queued = int(line_dict['time_queued'])

            # eligible time
            if any([c.isalpha() for c in line_dict['time_eligible']]):
                i_job.time_eligible = -1
            else:
                i_job.time_eligible = int(line_dict['time_eligible'])

            # end time
            if any([c.isalpha() for c in line_dict['time_end']]):
                i_job.time_end = -1
            else:
                i_job.time_end = int(line_dict['time_end'])

            # start time
            if any([c.isalpha() for c in line_dict['time_start']]):
                i_job.time_start = -1
            else:
                i_job.time_start = int(line_dict['time_start'])

            # average memory
            if any([c.isalpha() for c in line_dict['memory_kb'][:-2]]):
                i_job.memory_kb = -1
            else:
                i_job.memory_kb = int(line_dict['memory_kb'][:-2])

            if 'ncpus' in line_dict:
                i_job.ncpus = int(line_dict['ncpus'])
            else:
                i_job.ncpus = -1

            # i_job.cpu_percent = float(line_dict['cpu_percent'].replace(":", ""))
            i_job.group = str(line_dict['group'])
            i_job.jobname = str(line_dict['jobname'])
            i_job.user = str(line_dict['user'])

            pbs_jobs.append(i_job)

            cce += 1
        cc += 1

    return pbs_jobs
