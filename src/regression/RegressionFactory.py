

def RegrassionFactory(key, data):

    if key == "ANN":

        return RegressionANN(data)

    elif key == "Poly":

        #return ClusteringDBSCAN(data)

    else:

        raise ValueError('option not recognised!')
