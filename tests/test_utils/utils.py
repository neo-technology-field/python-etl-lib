from pathlib import Path


def get_test_file(file_name):
    """
    Returns the absolut path of a file that exists in `tests/data/` directory.
    """
    return Path(__file__).parent / f"../data/{file_name}"
