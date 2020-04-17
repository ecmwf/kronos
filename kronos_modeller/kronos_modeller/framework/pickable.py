try:
    import pickle as pickle
except ImportError:
    import pickle


class PickableObject(object):

    def __init__(self):
        pass

    def __unicode__(self):
        return "class name: {}".format(self.__class__.__name__)

    @classmethod
    def from_pickled(cls, pickle_name):
        with open(pickle_name, 'r') as f:
            return pickle.load(f)

    def export_pickle(self, pickle_name):
        with open(pickle_name, 'w') as f:
            return pickle.dump(self, f)