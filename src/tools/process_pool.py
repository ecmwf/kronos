"""
A modification of the multiprocessing.pool functionality to give:

i) A sensible notion of "global" data
ii) A modified imap, which has a hook to report when each object has finished processing (even when
    it has not been returned by the generator, as it is waiting for other objects to complete).
"""
import signal

from multiprocessing.pool import Pool, IMapIterator, RUN, mapstar
from tools.print_colour import print_colour

# The worker processes are only used for one task. We store the global data here to ensure that we
# don't have to worry about pickling the objects at any point other than initialisation, whilst retaining
# executable functions that are trivial.
_global_data = None
_processing_fn = None

# There is only one parent.
_global_parent = False


def _internal_initialiser(data, processing_fn):

    global _global_data
    assert _global_data is None
    _global_data = data

    global _processing_fn
    assert _processing_fn is None
    _processing_fn = processing_fn


def _internal_worker(object):
    """
    Make the global data available to the processing function
    """
    global _processing_fn
    global _global_data
    try:

        return _processing_fn(object, _global_data)

    except KeyboardInterrupt:
        # Catch and throw away KeyboardInterrupts, so that they propagate back to the master processes,
        # which can correctly terminate the workers
        pass



class IMapIteratorLocal(IMapIterator):
    """
    A modified version of IMapIterator, which calls a supplied callback when each of the work components
    completes, rather than when it is released back into the queue
    """
    def __init__(self, callback, *args, **kwargs):
        super(IMapIteratorLocal, self).__init__(*args, **kwargs)

        self.element_count = 0
        self.callback = callback

    def _set(self, i, obj):
        self.element_count += 1
        self.callback(self.element_count, i, obj[1])

        # print "Calling _set: {} {}".format(i, obj)
        super(IMapIteratorLocal, self)._set(i, obj)


class ProcessingPool(Pool):

    def __init__(self, processing_fn, callback, processes=1, global_data=None):

        global _global_parent
        _global_parent = True

        self.callback = callback

        super(ProcessingPool, self).__init__(
            processes=processes,
            initializer=_internal_initialiser,
            initargs=(global_data, processing_fn))

    def imap(self, iterable, chunksize=1):
        """
        Equivalent of `itertools.imap()` -- can be MUCH slower than `Pool.map()`
        """
        assert self._state == RUN
        if chunksize == 1:
            result = IMapIteratorLocal(self.callback, self._cache)
            self._taskqueue.put((((result._job, i, _internal_worker, (x,), {})
                                  for i, x in enumerate(iterable)), result._set_length))
            return result
        else:
            assert chunksize > 1
            task_batches = Pool._get_tasks(_internal_worker, iterable, chunksize)
            result = IMapIteratorLocal(self.callback, self._cache)
            self._taskqueue.put((((result._job, i, mapstar, (x,), {})
                                  for i, x in enumerate(task_batches)), result._set_length))
            return (item for chunk in result for item in chunk)


