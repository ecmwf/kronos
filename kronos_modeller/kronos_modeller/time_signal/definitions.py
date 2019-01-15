from collections import OrderedDict

# The availably types of time-series (or summed/averaged totals) data that we can use
signal_types = OrderedDict([

    # (file) I/O
    ('kb_read',  {'type': float, 'category': 'file-read',  'behaviour': 'sum'}),
    ('kb_write', {'type': float, 'category': 'file-write', 'behaviour': 'sum'}),
    ('n_read',   {'type': int,   'category': 'file-read',  'behaviour': 'sum'}),
    ('n_write',  {'type': int,   'category': 'file-write', 'behaviour': 'sum'}),

    # MPI activity
    ('n_pairwise',    {'type': int,   'category': 'mpi',  'behaviour': 'sum'}),
    ('kb_pairwise',   {'type': float, 'category': 'mpi',  'behaviour': 'sum'}),
    ('n_collective',  {'type': int,   'category': 'mpi',  'behaviour': 'sum'}),
    ('kb_collective', {'type': float, 'category': 'mpi',  'behaviour': 'sum'}),

    # # CPU
    ('flops', {'type': int, 'category': 'cpu', 'behaviour': 'sum'}),

    # # Memory allocated
    ('kb_mem', {'type': int, 'category': 'memory', 'behaviour': 'sum'})
])


# add print info
float_format_print = '%16.3f'
int_format_print = '%16.0f'

signal_types['flops']["print_info"] = {"unit": "Gflops", "format": int_format_print, "conv": 1.0/(1024.0**3)}
signal_types['kb_read']["print_info"] = {"unit": "Gbytes", "format": float_format_print, "conv": 1.0/(1024.0**2)}
signal_types['kb_write']["print_info"] = {"unit": "Gbytes", "format": float_format_print, "conv": 1.0/(1024.0**2)}
signal_types['n_read']["print_info"] = {"unit": "times ", "format": int_format_print, "conv": 1.0}
signal_types['n_write']["print_info"] = {"unit": "times ", "format": int_format_print, "conv": 1.0}
signal_types['n_pairwise']["print_info"] = {"unit": "times ", "format": int_format_print, "conv": 1.0}
signal_types['kb_pairwise']["print_info"] = {"unit": "Gbytes", "format": float_format_print, "conv": 1.0/(1024.0**2)}
signal_types['n_collective']["print_info"] = {"unit": "times ", "format": int_format_print, "conv": 1.0}
signal_types['kb_collective']["print_info"] = {"unit": "Gbytes", "format": float_format_print, "conv": 1.0/(1024.0**2)}
signal_types['kb_mem']["print_info"] = {"unit": "Gbytes", "format": int_format_print, "conv": 1.0/(1024.0**2)}

signal_types['flops']["print_info"]["raw_units"] = "FLOPS"
signal_types['kb_read']["print_info"]["raw_units"] = "KiB"
signal_types['kb_write']["print_info"]["raw_units"] = "KiB"
signal_types['n_read']["print_info"]["raw_units"] = "-"
signal_types['n_write']["print_info"]["raw_units"] = "-"
signal_types['n_pairwise']["print_info"]["raw_units"] = "-"
signal_types['kb_pairwise']["print_info"]["raw_units"] = "KiB"
signal_types['n_collective']["print_info"]["raw_units"] = "-"
signal_types['kb_collective']["print_info"]["raw_units"] = "KiB"
signal_types['kb_mem']["print_info"]["raw_units"] = "KiB"

signal_types['flops']["print_info"]["description"] = "Number of flops executed"
signal_types['kb_read']["print_info"]["description"] = "KiB read from disk"
signal_types['kb_write']["print_info"]["description"] = "KiB written to disk"
signal_types['n_read']["print_info"]["description"] = "Number of I/O read operations"
signal_types['n_write']["print_info"]["description"] = "Number of I/O write operations"
signal_types['n_pairwise']["print_info"]["description"] = "Number of point-to-point MPI operations"
signal_types['kb_pairwise']["print_info"]["description"] = "KiB passed in point-to-point MPI operations"
signal_types['n_collective']["print_info"]["description"] = "Number of collective MPI operations"
signal_types['kb_collective']["print_info"]["description"] = "KiB passed in collective MPI operations"
signal_types['kb_mem']["print_info"]["description"] = "KiB of allocated memory"


time_signal_names = signal_types.keys()