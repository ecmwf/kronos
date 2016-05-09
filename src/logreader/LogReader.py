import numpy as np
from pylab import *
import csv
import os


class LogReader:

    #===================================================================
    def __init__(self, Options):

        self.LogData = []
        self.LogList = []
        self.out_dir = Options.dir_output
        self.darshan_dir = Options.dir_log_output
        self.fname_csv_out = Options.fname_csv_out

    #===================================================================
    def read_logs(self):
        print "base methpd called"

    #===================================================================
    def write_csv(self):

        field_names = ['jobname',
                       'user',
                       'group',
                       'time_start',
                       'time_end',
                       'time_created',
                       'time_queued',
                       'time_eligible',
                       'time_start_0',
                       'time_mid_0',
                       'runtime',
                       'ncpus',
                       'memory_kb',
                       'cpu_percent',
                       'IO_N_read',
                       'IO_Kb_read',
                       'IO_N_write',
                       'IO_Kb_write',
                       ]

        f = open(self.out_dir + "/" + self.fname_csv_out, 'w')
        w = csv.DictWriter(f, field_names)
        w.writeheader()
        w.writerows(self.LogData)
        f.close()

    #===================================================================
    def make_plots(self):

        iFig = 1

        #------------ ncpu(t) -------------------
        xname = "time_start_0"
        yname = "ncpus"
        vecXkeys = np.asarray([x[xname] for x in self.LogData])
        vecYkeys = np.asarray([y[yname] for y in self.LogData])

        figure(iFig)
        title('x=' + str(xname) + ',  y=' + str(yname))
        plot(vecXkeys, vecYkeys, 'k.')
        xlabel(xname)
        ylabel(yname)

        savefig(self.out_dir + '/plot_' 'x=' + xname + '_y=' + yname + '.png')
        iFig = iFig + 1
        #----------------------------------------

        #------------ PDF(ncpus) ----------------
        yname = "ncpus"
        vecYkeys = np.asarray([y[yname] for y in self.LogData])
        nbins = 40
        hh, bin_edges = np.histogram(vecYkeys, bins=nbins, density=True)
        figure(iFig)
        title("PDF of ncpus")
        xx = (bin_edges[1:] + bin_edges[:-1]) / 2
        bar(bin_edges[:-1], hh, diff(bin_edges), color='b')
        yscale('log')
        savefig(self.out_dir + '/plot' + '_y=' + yname + '_hist.png')
        iFig = iFig + 1
        #----------------------------------------

        #------------ PDF(memory Kb) ---------------
        yname = "memory_kb"
        vecYkeys = np.asarray([y[yname] for y in self.LogData])
        nbins = 40
        hh, bin_edges = np.histogram(vecYkeys, bins=nbins, density=True)
        figure(iFig)
        title("PDF of memory_kb")
        xx = (bin_edges[1:] + bin_edges[:-1]) / 2
        bar(bin_edges[:-1], hh, diff(bin_edges), color='r')
        yscale('log')
        savefig(self.out_dir + '/plot' + '_y=' + yname + '_hist.png')
        iFig = iFig + 1
        #---------------------------------------

        #------------ PDF(runtime) ---------------
        yname = "runtime"
        vecYkeys = np.asarray([y[yname] for y in self.LogData])
        nbins = 40
        hh, bin_edges = np.histogram(vecYkeys, bins=nbins, density=True)
        figure(iFig)
        title("PDF of memory_kb")
        xx = (bin_edges[1:] + bin_edges[:-1]) / 2
        bar(bin_edges[:-1], hh, diff(bin_edges), color='g')
        yscale('log')
        savefig(self.out_dir + '/plot' + '_y=' + yname + '_hist.png')
        iFig = iFig + 1
        #---------------------------------------

        #---------- derived IO qtys ------------
        xname = "ncpus"
        vecXkeys = np.asarray([x[xname] for x in self.LogData])

        figure(iFig)
        title("IO quantities")

        #------ sub 1
        # subplot(4,1,1)
        yname = "ncpus"
        vecYkeys = np.asarray([y[yname] for y in self.LogData])
        plot(vecXkeys, vecYkeys, 'k*')

        yname = "IO_N_read"
        vecYkeys = np.asarray([y[yname] for y in self.LogData])
        plot(vecXkeys, vecYkeys, 'r.')

        yname = "IO_N_write"
        vecYkeys = np.asarray([y[yname] for y in self.LogData])
        plot(vecXkeys, vecYkeys, 'g.')
        #------------

        # yscale('log')
        savefig(self.out_dir + '/plot' + '_y=' + "IOderiv" + '_hist.png')
        iFig = iFig + 1
        #---------------------------------------
