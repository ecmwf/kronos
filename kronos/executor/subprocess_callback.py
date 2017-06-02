import threading
import subprocess


class SubprocessManager(object):
    """
    Provide a mechanism to launch asynchronous threads:

    i) subprocess_callback runs a specified external process, and calls the callback with the
       stdout produced.
    """
    def __init__(self, maximum_workers=None):
        self.maximum_workers = maximum_workers

        self.threads_cv = threading.Condition()
        self.threads = set()

    def subprocess_callback(self, callback, *args):
        """
        Call subprocess.check_output asynchronously. A new thread is called to do the execution and wait
        in. Once the call has returned, the callback is called and passed the output as a parameter.
        """
        def threaded_fn(callback, args):
            try:
                output = subprocess.check_output(args)
                callback(output)
            except OSError:
                print "Command not found {}".format(args[0])
            except subprocess.CalledProcessError as e:
                print "Error in process {}".format(args[0])
                print "Output: {}".format(e.output)

            # Ensure that the thread is removed from the list.
            finally:
                self.threads_cv.acquire()
                self.threads.remove(threading.current_thread())
                self.threads_cv.notify()
                self.threads_cv.release()

        # Wait until there is space in the workers set
        self.threads_cv.acquire()
        if len(self.threads) >= self.maximum_workers:
            self.threads_cv.wait()

        # Start the thread and add it to the set
        thread = threading.Thread(target=threaded_fn, args=(callback, args))
        thread.start()
        self.threads.add(thread)

        self.threads_cv.release()

    @property
    def num_running(self):
        return len(self.threads)

    def wait_until(self, nthreads):
        """
        Wait until there are nthreads, or fewer, threads running
        """
        self.threads_cv.acquire()
        while len(self.threads) > nthreads:
            self.threads_cv.wait()
        self.threads_cv.release()

    def wait(self):
        """
        Wait until all of the threads (that have been started at this point) have returned
        """
        print "Waiting for {} worker threads to complete".format(len(self.threads))
        self.wait_until(0)

