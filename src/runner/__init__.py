from runner import simple
from runner import feedback

runner_list = {
    "simple": simple.SimpleRunner,
    "feedback": feedback.FeedbackLoopRunner,
}


def factory(key, config):

    return runner_list[key](config)
