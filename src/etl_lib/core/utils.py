def merge_summery(summery_1: dict, summery_2: dict) -> dict:
    """
    Helper function to merge dicts. Assuming that values are numbers.
    If a key exists in both dicts, then the result will contain a key with the added values.
    """
    return {i: summery_1.get(i, 0) + summery_2.get(i, 0)
            for i in set(summery_1).union(summery_2)}
