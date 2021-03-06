#!/usr/bin/env python

import os

from .global_config import global_config


# Global configuration (configured by CMake)


def human_readable_bytes(num):
    """
    Return a string associated with the current number of bytes in a human readable form.
    """
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi']:
        if abs(num) < 1024.0 or unit == 'Yi':
            break
        num /= 1024.0
    return "{:.3f} {}B".format(num, unit)


def enumerate_cache_files(path, multiplicity=None, size_min=None, size_max=None):
    """
    Enumerate the read cache files below the specified path

    :return: A generator of (file_name, file_size) tuples
    """
    if not multiplicity:
        multiplicity = global_config['read_file_multiplicity']
    if not size_min:
        size_min = global_config['read_file_size_min']
    if not size_max:
        size_max = global_config['read_file_size_max']

    print("Multiplicity of read files: {}".format(multiplicity))
    print("Read file size: 2^{} B, {}".format(size_max, human_readable_bytes(2 ** size_max)))

    size = 2 ** size_max

    for count in range(multiplicity):
        fn = "read-cache-{}".format(count)
        file = os.path.join(path, fn)

        yield (file, size)


def test_read_cache(path, multiplicity=None, min_size=None, max_size=None):
    """
    Test that the directory 'path' is initialised properly

    :return: True if correct, else False
    """
    for filename, size in enumerate_cache_files(path, multiplicity, min_size, max_size):

        if not (os.path.exists(filename) and os.path.isfile(filename)):
            print("File missing: {}".format(filename))
            return False

        real_size = os.path.getsize(filename)
        if real_size != size:
            print("File size incorrect for {}. Expected {}, found {}".format(filename, size, real_size))
            return False

    return True


def generate_read_cache(path, multiplicity=None, size_min=None, size_max=None):
    """
    In the directory 'path', generate the read cache files
    """
    if not os.path.exists(path):
        os.makedirs(path)

    for filename, size in enumerate_cache_files(path, multiplicity, size_min, size_max):

        with open(filename, 'w') as f:

            # Note that just using truncate creates a sparse file, as will any method that just seeks and writes
            # a small number of bytes. We need to force there to be actual file output to generate actual files
            # of the right size --- necessary as we want to test read performance in benchmarking...

            f.write('\0' * size)
            # f.truncate(size)
