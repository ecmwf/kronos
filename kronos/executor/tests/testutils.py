"""
Some utilities to make the testing easier!
"""

import uuid
import os


def scratch_tmpdir():
    """
    Generate a unique scratch temporary dir under the path specified in the environment variable TMPDIR.

    If TMPDIR is not set, this will raise an exception, triggering test failures.
    """
    tmpdir = os.environ.get('TMPDIR', '/var/tmp')
    if not (os.path.exists(tmpdir) and os.path.isdir(tmpdir)):
        raise RuntimeError("No temporary directory could be determined on this system")

    return os.path.join(tmpdir, uuid.uuid4().hex)

