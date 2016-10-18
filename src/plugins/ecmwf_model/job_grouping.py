import os

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

from jobs import ModelJob, concatenate_modeljobs


def grouping_by_tree_level(matching_jobs, settings_dict):
    """ grouping low-level jobs in the tree.. """

    print "grouping low-level jobs in the tree.."

    job_groups = {}
    for j in matching_jobs:

        # if it is a deep node, group it..
        if len(j.label.split('/')) > settings_dict['n_tree_levels']:
            root_label = ''.join(j.label.split('/')[:settings_dict['n_tree_levels']])
            if root_label in job_groups.keys():
                job_groups[root_label].append(j)
            else:
                job_groups[root_label] = [j]
        else:  # otherwise add it into the list as it is..
            root_label = ''.join(j.label.split('/'))
            if root_label in job_groups.keys():
                job_groups[root_label].append(j)
            else:
                job_groups[root_label] = [j]

    print "job grouping done!!"

    print "creating the grouped model jobs.."
    grouped_model_jobs = []
    label_cc = 0
    for k in job_groups.keys():
        # if it is just one job, append it
        if len(job_groups[k]) == 1:
            grouped_model_jobs.append(job_groups[k][0])
        else:  # group the jobs into one sa only..
            cat_job = concatenate_modeljobs('grouped-job-{}'.format(label_cc), job_groups[k])
            grouped_model_jobs.append(cat_job)
            label_cc += 1

    print "grouped model jobs created!"

    return grouped_model_jobs
