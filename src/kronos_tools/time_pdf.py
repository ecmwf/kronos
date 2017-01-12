import numpy as np


def equiv_time_pdf_exact(input_times, global_t0, global_tend, output_duration, output_ratio, n_bins):
    """
    Function that returns a distribution of times according to a given PDF
    :param input_times:
    :param global_t0:
    :param global_tend:
    :param output_duration:
    :param output_ratio:
    :param n_bins:
    :return:
    """

    assert (min(input_times) >= global_t0)
    assert (max(input_times) <= global_tend)

    # find the PDF of jobs start times
    input_time_min = min(input_times)
    input_time_max = max(input_times)
    input_time_duration = (input_time_max - input_time_min)
    input_time_duration_rel = input_time_duration/(global_tend-global_t0)
    input_time_after_t0_rel = (input_time_min - global_t0)/(global_tend-global_t0)
    input_time_bins = np.linspace(input_time_min, input_time_max, n_bins+1)
    input_time_pdf, _ = np.histogram(input_times, input_time_bins, density=False)

    # then calculate an "exact" distribution of time start from the provided PDF
    output_time_pdf = input_time_pdf
    output_time_bins_01 = (input_time_bins-min(input_time_bins))/(max(input_time_bins)-min(input_time_bins))
    output_time_bins = (output_time_bins_01*input_time_duration_rel + input_time_after_t0_rel) * output_duration

    output_times = np.asarray([])
    output_times_pdf_actual = np.zeros(n_bins)

    for bb in range(n_bins):
        y_min = output_time_bins[bb]
        y_max = output_time_bins[bb + 1]
        n_sa_bin = int(output_time_pdf[bb]*output_ratio*output_duration/input_time_duration)
        random_y_values = y_min + np.random.rand(n_sa_bin) * (y_max - y_min)
        output_times = np.append(output_times, random_y_values)
        output_times_pdf_actual[bb] = n_sa_bin

    return output_times, output_times_pdf_actual, output_time_bins


def equiv_time_pdf(input_times, global_t0, global_tend, output_duration, output_ratio, n_bins):
    """
    Function that returns a "random" distribution of times according to a given PDF
    :param input_times:
    :param global_t0:
    :param global_tend:
    :param output_duration:
    :param output_ratio:
    :param n_bins:
    :return:
    """

    assert (min(input_times) >= global_t0)
    assert (max(input_times) <= global_tend)

    # calculate the submit rate from the selected workload
    real_submit_rate = float(len(input_times)) / (max(input_times) - min(input_times))
    requested_submit_rate = real_submit_rate * output_ratio
    n_modelled_jobs = int(requested_submit_rate * output_duration)

    # find the PDF of jobs start times
    input_time_min = min(input_times)
    input_time_max = max(input_times)
    input_time_duration_rel = (input_time_max - input_time_min)/(global_tend-global_t0)
    input_time_after_t0_rel = (input_time_min - global_t0)/(global_tend-global_t0)
    input_time_bins = np.linspace(input_time_min, input_time_max, n_bins+1)
    # input_time_bins_mid = (input_time_bins[:-1] + input_time_bins[1:]) / 2.0

    input_time_pdf, _ = np.histogram(input_times, input_time_bins, density=False)

    # then calculate an "exact" distribution of time start from the provided PDF
    output_time_pdf = input_time_pdf
    output_time_bins_01 = (input_time_bins-min(input_time_bins))/(max(input_time_bins)-min(input_time_bins))
    output_time_bins = (output_time_bins_01*input_time_duration_rel + input_time_after_t0_rel) * output_duration
    output_time_bins_mid = (output_time_bins[:-1] + output_time_bins[1:]) / 2.0

    # # generate the output vector from the PDF distribution
    # print "-----------------------------------"
    # print "len(input_times)", len(input_times)
    # print "n_modelled_jobs", n_modelled_jobs
    # print "output_time_pdf", output_time_pdf
    # print "output_time_bins_mid", output_time_bins_mid
    # print "-----------------------------------"

    output_times = np.random.choice(output_time_bins_mid, p=output_time_pdf/float(sum(output_time_pdf)), size=n_modelled_jobs)
    output_times_pdf_actual, _ = np.histogram(output_times, output_time_bins, density=False)

    return output_times, output_times_pdf_actual, output_time_bins