

def max_not_none(*args):
    """
    If all of the elements are None, return None, otherwise return the largest non-None element
    """
    if any(x is not None for x in args):
        return max(x for x in args if x is not None)
    return None


def min_not_none(*args):
    """
    If all of the elements are None, return None, otherwise return the smallest non-None element
    """
    if any(x is not None for x in args):
        return min(x for x in args if x is not None)
    return None
