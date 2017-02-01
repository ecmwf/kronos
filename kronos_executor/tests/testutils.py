"""
Some utilities to make the testing easier!
"""

import uuid
import os


def scratch_tmpdir():
    """
    Generate a unique scratch temporary dir under the path specified in the environment variable TMPDIR.

    If TMPDIR is not set, this will raise a KeyError exception, triggering test failures.
    """
    return os.path.join(os.environ['TMPDIR'], uuid.uuid4().hex)
