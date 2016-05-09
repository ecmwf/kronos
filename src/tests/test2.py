
#////////////////////////////////////////////////////////////////
def test2():

    #================================================================
    ConfigOptions = Config()
#    plot_tag = "test5_FFT_Kmeans_SELCLUST"
#    plot_tag = "test5_FFT_SOM"
    plot_tag = "test_NEW_"

    #================================================================
    InputWorkload = RealWorkload(ConfigOptions)
    InputWorkload.read_PBS_logs("/perm/ma/maab/PBS_log_example/20151123_test_2k")
#    InputWorkload.read_PBS_logs("/perm/ma/maab/PBS_log_example/20151123_test100")
    InputWorkload.calculate_derived_quantities()
    InputWorkload.make_plots(plot_tag)

    #================================================================
    Corrector = WorkloadCorrector(InputWorkload, ConfigOptions)
    Corrector.replace_missing_data("ANN")
    #Corrector.enrich_data_with_TS("FFT")
    Corrector.enrich_data_with_TS("bins")
    Corrector.calculate_global_metrics()
    Corrector.plot_missing_data(plot_tag)
    Corrector.make_plots(plot_tag)

    #================================================================
    Model = IOWSModel(ConfigOptions)
    Model.set_input_workload(InputWorkload)
    
    #------------ cases with spacified number of clusters --------------
    for iNC in np.append(np.arange(1, len(InputWorkload.LogData) + 1, 
                                   int(len(InputWorkload.LogData) / 5)), 
                                   len(InputWorkload.LogData)):
                                       
        Model.apply_clustering("time_plane", "Kmeans", iNC)
        #Model.apply_clustering("spectral", "Kmeans", iNC)
        #Model.apply_clustering("spectral", "DBSCAN")
        Model.make_plots( plot_tag )


    #================================================================
    PlotHandler.print_fig_handle_ID()
    
#////////////////////////////////////////////////////////////////


#////////////////////////////////////////////////////////////////
if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    import numpy as np
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    
    from config.config import Config
    from RealWorkload import RealWorkload
    from IOWSModel import IOWSModel
    from WorkloadCorrector import WorkloadCorrector
    from PlotHandler import PlotHandler

    test2()
#////////////////////////////////////////////////////////////////









