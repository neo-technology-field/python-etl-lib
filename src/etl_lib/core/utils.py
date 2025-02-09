import logging


def merge_summery(summery_1: dict, summery_2: dict) -> dict:
    """
    Helper function to merge dicts. Assuming that values are numbers.
    If a key exists in both dicts, then the result will contain a key with the added values.
    """
    return {i: summery_1.get(i, 0) + summery_2.get(i, 0)
            for i in set(summery_1).union(summery_2)}


def setup_logging(log_file=None):
    """
    Set up logging to console and optionally to a log file.

    :param log_file: Path to the log file
    :type log_file: str, optional
    """
    handlers = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=handlers
    )
