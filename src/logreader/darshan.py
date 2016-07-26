from pylab import *

import os
import subprocess

from jobs import IngestedJob, ModelJob
from logreader.base import LogReader
from logreader.dataset import IngestedDataSet
from tools.print_colour import print_colour


class DarshanDataSet(IngestedDataSet):

    def model_jobs(self):
        """
        Model the Darshan jobs, given a list of injested jobs

        """

        # TODO: Do the aggregation
        for job in self.joblist:
            yield ModelJob(
                time_start=0,
                duration=0,
                ncpus=1,
                nnodes=1,
                time_series=[]
            )


class DarshanIngestedJob(IngestedJob):
    """
    N.B. Darshan may produce MULTIPLE output files for each of the actual HPC jobs (as it produces one per command
    that is run in the submit script).
    """
    class File(object):
        """
        An object to represent the file information available in a Darshan job
        """
        def __init__(self, name):
            self.name = name

            self.bytes_read = 0
            self.bytes_written = 0
            self.open_count = 0
            self.write_count = 0
            self.read_count = 0

            self.read_time = None
            self.write_time = None

        def __unicode__(self):
            return "File({} reads, {} bytes, {} writes, {} bytes)".format(self.read_count, self.bytes_read, self.write_count, self.bytes_written)

        def __str__(self):
            return unicode(self).encode('utf-8')

    def __init__(self, label=None, file_details=None):
        super(DarshanIngestedJob, self).__init__(label)

        assert file_details is not None
        self.file_details = file_details

    def aggregate(self, job):
        """
        Combine two ingested jobs together, as Darshan produces one file per command run inside the job script
        (and all these should be together).
        """
        assert self.label == job.label


class DarshanLogReader(LogReader):

    job_class = DarshanIngestedJob
    dataset_class = DarshanDataSet
    file_pattern = "*.gz"

    # By default we end up with a whole load of darshan logfiles within a directory.
    label_method = "directory"

    darshan_params = {
        'uid': 'uid',
        'jobid': 'jobid',
        'nprocs': 'nprocs',
        'start_time': 'time_start',
        'end_time': 'time_end',
        'darshan log version': 'log_version'
    }

    # See darshan summary on cca/ccb in the darshan module
    file_params = {
        'CP_BYTES_READ': 'bytes_read',
        'CP_BYTES_WRITTEN': 'bytes_written',
        'CP_POSIX_OPENS': 'open_count',
        'CP_POSIX_FOPENS': 'open_count',
        'CP_POSIX_READ_TIME': 'read_time',
        'CP_POSIX_WRITE_TIME': 'write_time',
        'CP_POSIX_WRITES': 'write_count',
        'CP_POSIX_FWRITES': 'write_count',
        'CP_POSIX_READS': 'read_count',
        'CP_POSIX_FREADS': 'read_count'

        # CP_SIZE_AT_OPEN
        # CP_MODE
        # CP_POSIX_FSEEKS, CP_POSIX_SEEKS
        # CP_POSIX_STATS
        # CP_POSIX_FSYNCS
        # CP_F_POSIX_META_TIME, CP_F_MPI_META_TIME
    }

    def __init__(self, path, **kwargs):

        # TODO: Configure the darshan paths (need darshan-parser)
        print "Darshan Log Reader"

        # Custom configuration:
        self.parser_command = kwargs.pop('parser', 'darshan-parser')

        super(DarshanLogReader, self).__init__(path, **kwargs)

    def read_log(self, filename, suggested_label):
        """
        Read a darshan log!
        """
        # try:
        #     output = subprocess.check_output([self.parser_command, filename])
        # except subprocess.CalledProcessError as e:

        pipes = subprocess.Popen([self.parser_command, filename], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = pipes.communicate()

        if len(error) != 0:
            print_colour("orange", "\n{}".format(error.strip()), flush=True)

        if pipes.returncode != 0:
            if len(error) == 0:
                print ""
            print_colour("red", "Got an error: {} - {}".format(pipes.returncode, filename), flush=True)

            # Just skip this (with warnings), as the Darshan data is only being used in conjunction with something
            # else, so there won't be a blank.
            return []

        files = {}
        params = {
            'label': suggested_label
        }

        for line in output.splitlines():

            trimmed_line = line.strip()

            if len(trimmed_line) == 0:
                pass
            elif trimmed_line[0] == '#':
                split = trimmed_line.split(':', 1)
                key = split[0][1:].strip()
                job_key = self.darshan_params.get(key, None)
                if job_key:
                    params[job_key] = split[1].split()[0].strip()
            else:
                # A data line
                bits = trimmed_line.split()
                # file = ' '.join(bits[4:])
                file = bits[4]

                # Add the file to the map if required
                if file not in files:
                    files[file] = DarshanIngestedJob.File(file)

                file_elem = self.file_params.get(bits[2], None)
                if file_elem is not None:
                    setattr(files[file], file_elem, getattr(files[file], file_elem) + int(bits[3]))

        params['files'] = files

        return [self.job_class(label=suggested_label, file_details=files)]

    def read_logs_generator(self):
        """
        Darshan produces one log per command executed in the script. This results in multiple Darshan files per
        job, which need to be aggregated. Each of the jobs will be sequential, so we combine them.
        """

        current_job = None

        for job in super(DarshanLogReader, self).read_logs_generator():

            if current_job is None:
                current_job = job

            elif job.label == current_job.label:
                current_job.aggregate(job)

            else:
                yield current_job
                current_job = job

        # And when we are at the end of the list, yield the current job
        if current_job is not None:
            yield current_job

#
#    #===================================================================
#    def __init__(self, Options):
#        LogReader.__init__(self, Options)
#        #self.darshan_dir  = Options.darshan_dir
#    #===================================================================
#
#    #===================================================================
#    def read_logs(self):
#
#        dsh_files = [f for f in os.listdir(self.darshan_dir) if (
#            os.path.isfile(os.path.join(self.darshan_dir, f)) and ("darshan.gz" in f))]
#
#        os.system("module load darshan")
#        darshan_parser = "/usr/local/apps/darshan/2.3.1-ecm1.1/bin/darshan-parser"
#
#        if not os.path.isfile(darshan_parser):
#            print("darshan module not loaded: darshan-parser not found in the path")
#
#        #--------- init ----------
#        systemfiles = 0
#        system = ('/usr/',
#                  '/proc/',
#                  '/sys/',
#                  '/dev/',
#                  '/etc/',
#                  '/boot/',
#                  )
#        job_cc = 0
#        #-------------------------
#
#        #------- loop over darshan files.. ------
#        for dsh_fname in dsh_files:
#
#            print dsh_fname
#
#            #-- init dictionary ----
#            line_dict = {}
#
#            dir_file_name = self.darshan_dir + "/" + dsh_fname
#            tmpfile = dir_file_name + ".ascii"
#            print "here: " + tmpfile
#            command = "%s %s > %s" % (darshan_parser, dir_file_name, tmpfile)
#            print "command: " + command
#
#            proc = subprocess.Popen(
#                command, shell=True, stderr=subprocess.PIPE)
#            stdout, stderr = proc.communicate()
#
#            if stderr != "":
#                print(stderr)
#                sys.exit(2)
#
#            try:
#                print "here" + tmpfile
#                f = open(tmpfile, "r")
#            except IOError as e:
#                print(
#                    "I/O error({0}): {1} --->   %s".format(e.errno, e.strerror) % tmpfile)
#                usage()
#                sys.exit(2)
#
#            totals = {
#                'nfwrites': 0,
#                'nfread': 0,
#                'tbytesread': 0,
#                'tbyteswritten': 0,
#                'tfile_reads': 0,
#                'tfile_writes': 0,
#                'tnwrites': 0,
#                'tnreads': 0,
#                'tnopens': 0,
#                'tnstats': 0,
#                'tnseeks': 0,
#                'tnfsyncs': 0,
#                'tfsynctime': 0,
#                'treadtime': 0,
#                'twritetime': 0,
#                'tmetatime': 0,
#                'twastemeta': 0,
#                'twastemetatime': 0,
#                'topenclose': 0,
#                'topenclosetime': 0, }
#
#            fileaccess = {}
#            sfiles = {}
#
#            for line in f:
#                nopens = 0
#                bytesread = 0
#                byteswritten = 0
#                nopens = 0
#                nstats = 0
#                nseeks = 0
#                nfsyncs = 0
#                readtime = 0
#                writetime = 0
#                metatime = 0
#                if line.startswith("# nprocs:"):
#                    nprocs = line[10:]
#                    continue
#                elif line.startswith("# exe:"):
#                    exe = line[7:]
#                    continue
#                elif line.startswith("# jobid:"):
#                    jobid = line[10:]
#                    continue
#
#                # elif line.startswith("# start_time_asci:"):
#                    # starttime=line[19:]
#                    # continue
#                # elif line.startswith("# end_time_asci:"):
#                    # endtime=line[17:]
#                    # continue
#                elif line.startswith("# start_time:"):
#                    starttime = line[14:]
#                    continue
#                elif line.startswith("# end_time:"):
#                    endtime = line[12:]
#                    continue
#
#                elif line.startswith("# run time:"):
#                    runtime = line[12:]
#                    continue
#                elif line.startswith("#") or line.startswith("\n"):
#                    continue
#                else:
#                    larray = line.split("\t")
#                    rank = int(larray[0])
#                    fname = larray[4][3:]
#                    if not systemfiles:
#                        fbreak = 0
#                        for pth in system:
#                            if fname.startswith(pth):
#                                # print "comensa per %s" % pth
#                                fbreak = 1
#                                continue
#                    else:
#                        fbreak = 1
#                        for pth in system:
#                            if fname.startswith(pth):
#                                # print "no comensa per %s" % pth
#                                fbreak = 0
#                                continue
#                    if fbreak:
#                        continue
#                    if rank not in fileaccess:
#                        fileaccess[rank] = {}
#                    if larray[4] not in fileaccess[rank] and float(larray[3]) > 0:
#                        fileaccess[rank][larray[4]] = {}
#                    if larray[2] == 'CP_BYTES_READ':
#                        bytesread = float(larray[3])
#                        if bytesread > 0:
#                            totals['tbytesread'] += bytesread
#                            fileaccess[rank][larray[4]][
#                                'bytesread'] = bytesread
#                    elif larray[2] == 'CP_BYTES_WRITTEN':
#                        byteswritten = float(larray[3])
#                        if byteswritten > 0:
#                            totals['tbyteswritten'] += byteswritten
#                            fileaccess[rank][larray[4]][
#                                'byteswritten'] = byteswritten
#                    elif larray[2] == 'CP_POSIX_OPENS' or larray[2] == 'CP_POSIX_FOPENS':
#                        nopens = int(larray[3])
#                        if nopens > 0:
#                            totals['tnopens'] += nopens
#                            fileaccess[rank][larray[4]]['nopens'] = nopens
#                    elif larray[2] == 'CP_POSIX_STATS':
#                        nstats = int(larray[3])
#                        if nstats > 0:
#                            totals['tnstats'] += nstats
#                            fileaccess[rank][larray[4]]['nstats'] = nstats
#                    elif larray[2] == 'CP_SIZE_AT_OPEN':
#                        fsize = float(larray[3])
#                        if fsize > 0:
#                            fileaccess[rank][larray[4]]['fsize'] = fsize
#                    elif larray[2] == 'CP_MODE':
#                        openmode = int(larray[3])
#                        if openmode != 0:
#                            fileaccess[rank][larray[4]]['openmode'] = openmode
#                    elif larray[2] == 'CP_POSIX_FSEEKS' or larray[2] == 'CP_POSIX_SEEKS':
#                        nseeks = int(larray[3])
#                        if nseeks > 0:
#                            totals['tnseeks'] += nseeks
#                            fileaccess[rank][larray[4]]['nseeks'] = nseeks
#                    # or larray[2] == 'CP_F_MPI_READ_TIME':
#                    elif larray[2] == 'CP_F_POSIX_READ_TIME':
#                        readtime = float(larray[3])
#                        if readtime > 0:
#                            totals['treadtime'] += readtime
#                            totals['tfile_reads'] += 1
#                            fileaccess[rank][larray[4]]['readtime'] = readtime
#                    # or larray[2] == 'CP_F_MPI_WRITE_TIME':
#                    elif larray[2] == 'CP_F_POSIX_WRITE_TIME':
#                        writetime = float(larray[3])
#                        if writetime > 0:
#                            if 'nwrites' not in fileaccess[rank][larray[4]]:
#                                totals['tfsynctime'] += writetime
#                                fileaccess[rank][larray[4]][
#                                    'fsynctime'] = writetime
#                            else:
#                                totals['twritetime'] += writetime
#                                totals['tfile_writes'] += 1
#                                fileaccess[rank][larray[4]][
#                                    'writetime'] = writetime
#                    elif larray[2] == 'CP_POSIX_WRITES':
#                        nwrites = int(larray[3])
#                        if nwrites > 0:
#                            totals['tnwrites'] += nwrites
#                            fileaccess[rank][larray[4]]['nwrites'] = nwrites
#                    elif larray[2] == 'CP_POSIX_READS':
#                        nreads = int(larray[3])
#                        if nreads > 0:
#                            totals['tnreads'] += nreads
#                            fileaccess[rank][larray[4]]['nreads'] = nreads
#                    elif larray[2] == 'CP_POSIX_FSYNCS':
#                        fsyncs = int(larray[3])
#                        if fsyncs > 0:
#                            totals['tnfsyncs'] += fsyncs
#                            fileaccess[rank][larray[4]]['fsyncs'] = fsyncs
#                    elif larray[2] == 'CP_F_POSIX_META_TIME' or larray[2] == 'CP_F_MPI_META_TIME':
#                        metatime = float(larray[3])
#                        if metatime > 0:
#                            fileaccess[rank][larray[4]]['metatime'] = metatime
#                            totals['tmetatime'] += metatime
#                            # check rates and information
#                            if 'writetime' not in fileaccess[rank][larray[4]] and 'readtime' not in fileaccess[rank][larray[4]]:
#                                if 'nopens' not in fileaccess[rank][larray[4]]:
#                                    fileaccess[rank][larray[4]][
#                                        'wastemeta'] = 1
#                                    totals['twastemeta'] += 1
#                                    totals['twastemetatime'] += metatime
#                                else:
#                                    fileaccess[rank][larray[4]][
#                                        'openclose'] = 1
#                                    totals['topenclose'] += 1
#                                    totals['topenclosetime'] += metatime
#                            else:
#                                # the file has been opened for read/write, good
#                                if 'readtime' in fileaccess[rank][larray[4]] and 'bytesread' in fileaccess[rank][larray[4]] and fileaccess[rank][larray[4]]['bytesread'] > 0:
#                                    fileaccess[rank][larray[4]]['readrate'] = mytools.mb(
#                                        fileaccess[rank][larray[4]]['bytesread']) / fileaccess[rank][larray[4]]['readtime']
#                                if 'writetime' in fileaccess[rank][larray[4]] and 'byteswritten' in fileaccess[rank][larray[4]] and fileaccess[rank][larray[4]]['byteswritten'] > 0:
#                                    fileaccess[rank][larray[4]]['writerate'] = mytools.mb(fileaccess[rank][larray[4]][
#                                                                                          'byteswritten']) / fileaccess[rank][larray[4]]['writetime']
#
#                    elif larray[2] == 'CP_POSIX_FWRITES':
#                        nwrites = int(larray[3])
#                        #idx_last_slash = [m.start() for m in re.finditer('/', ss)][-1]
#                        totals['nfwrites'] += nwrites
#
#                    elif larray[2] == 'CP_POSIX_FREADS':
#                        nreads = int(larray[3])
#                        #idx_last_slash = [m.start() for m in re.finditer('/', ss)][-1]
#                        totals['nfread'] += nreads
#
#            f.close()
#            os.remove(tmpfile)
#
#            print "################################################################"
#            print "##########################JOB RESUME############################"
#            print "Executable: \t%s" % (exe[:-1])
#            print "Nprocs (-n):\t%s" % (nprocs[:-1])
#            print "JOB ID:     \t%s" % (jobid[:-1])
#            print "Start Time: \t%s" % (starttime[:-1])
#            print "End Time:   \t%s" % (endtime[:-1])
#            print "Run Time:   \t%s" % (runtime[:-1])
#            print "################################################################"
#
#            #------------- fill up fields ---------------
#            line_dict["user"] = "dummy_user"
#            line_dict['group'] = 'default-group'
#            line_dict['jobname'] = (exe[:-1])
#            line_dict['time_created'] = -1
#            line_dict['time_queued'] = -1
#            line_dict['time_eligible'] = -1
#            line_dict['time_end'] = float(endtime)
#            line_dict['time_start'] = float(starttime)
#            line_dict["runtime"] = line_dict[
#                'time_end'] - line_dict['time_start']
#            line_dict['ncpus'] = int(nprocs[:-1])
#            line_dict['memory_kb'] = -1
#            line_dict['cpu_percent'] = -1
#            line_dict['IO_N_read'] = totals['nfread']
#            line_dict['IO_Kb_read'] = totals['tbytesread']
#            line_dict['IO_N_write'] = totals['nfwrites']
#            line_dict['IO_Kb_write'] = totals['tbyteswritten']
#            #--------------------------------------------
#
#            self.LogData.append(line_dict)
#            job_cc = job_cc + 1
#
#        #------ data aggregated per job ID -------
#        self.LogData = multikeysort(self.LogData, ['time_start'])
#
#        minStartTime = min(self.LogData, key=lambda x: x['time_start'])
#
#        for r in self.LogData:
#            r['time_start_0'] = r['time_start'] - minStartTime['time_start']
#            r['time_mid_0'] = r['time_start'] - minStartTime['time_start'] + \
#                (r['time_end'] - r['time_start']) / 2.0
#       #===================================================================
