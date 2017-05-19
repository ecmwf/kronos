from collections import OrderedDict

# The availably types of time-series (or summed/averaged totals) data that we can use
signal_types = OrderedDict([

    # # CPU
    ('flops',    {'type': int,   'category': 'cpu',        'behaviour': 'sum'}),

    # (file) I/O
    ('kb_read',  {'type': float, 'category': 'file-read',  'behaviour': 'sum'}),
    ('kb_write', {'type': float, 'category': 'file-write', 'behaviour': 'sum'}),
    ('n_read',   {'type': int,   'category': 'file-read',  'behaviour': 'sum'}),
    ('n_write',  {'type': int,   'category': 'file-write', 'behaviour': 'sum'}),

    # MPI activity
    ('n_pairwise',    {'type': int,   'category': 'mpi',  'behaviour': 'sum'}),
    ('kb_pairwise',   {'type': float, 'category': 'mpi',  'behaviour': 'sum'}),
    ('n_collective',  {'type': int,   'category': 'mpi',  'behaviour': 'sum'}),
    ('kb_collective', {'type': float, 'category': 'mpi',  'behaviour': 'sum'})
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


time_signal_names = signal_types.keys()