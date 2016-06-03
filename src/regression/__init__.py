from base import Base
from ANN import ANN


def factory(key, data):

    workers = {
        "ANN": ANN,
    }

    return workers[key](data)
