

class TimeTicker(object):
    """
    This class takes care of ticking the elapsed time,
    in particular, it provides time events for each elapsed second,
    regardless of how often is called, guaranteeing that all
    the "per-second" events are correctly and sequentially logged.
    """

    def __init__(self, time_0, keep_all_times=False):

        # time at the start of the recording..
        self.time_0 = time_0

        # keep count of which times have been correctly logged,
        # so that every time a new time arrives, only the unchecked part
        # of the logged times is checked
        self.last_logged_second = -1

        self.keep_all_times = keep_all_times
        if self.keep_all_times:
            self.all_times = []

    def get_elapsed_seconds(self, time_now):
        """
        Returns a list of elapsed seconds since last call
        :param time_now:
        :return:
        """

        sec_elapsed = int((time_now - self.time_0).total_seconds())

        elapsed_events_batch = []
        if sec_elapsed > self.last_logged_second:

            # new batch of per-second time events
            elapsed_events_batch = range(self.last_logged_second+1, sec_elapsed+1)

            # update last logged second
            self.last_logged_second = sec_elapsed

        if self.keep_all_times:
            self.all_times.extend(elapsed_events_batch)

        return elapsed_events_batch

