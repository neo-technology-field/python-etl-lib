import csv
import gzip
from pathlib import Path
from typing import Generator

from etl_lib.core.BatchProcessor import BatchProcessor, BatchResults
from etl_lib.core.Task import Task


class CSVBatchSource(BatchProcessor):
    """
    BatchProcessor that reads a CSV file using the `csv` package.

    File can optionally be gzipped.
    The returned batch of rows will have an additional `_row` column, containing the source row of the data,
    starting with 0.
    """

    def __init__(self, csv_file: Path, context, task: Task = None, **kwargs):
        """
        Constructs a new CSVBatchSource.

        Args:
            csv_file: Path to the CSV file.
            context: :class:`etl_lib.core.ETLContext.ETLContext` instance.
            kwargs: Will be passed on to the `csv.DictReader` providing a way to customise the reading to different
                csv formats.
        """
        super().__init__(context, task)
        self.csv_file = csv_file
        self.kwargs = kwargs

    def get_batch(self, max_batch__size: int) -> Generator[BatchResults]:
        for batch_size, chunks_ in self.__read_csv(self.csv_file, batch_size=max_batch__size, **self.kwargs):
            yield BatchResults(chunk=chunks_, statistics={"csv_lines_read": batch_size}, batch_size=batch_size)

    def __read_csv(self, file: Path, batch_size: int, **kwargs):
        if file.suffix == ".gz":
            with gzip.open(file, "rt", encoding='utf-8-sig') as f:
                yield from self.__parse_csv(batch_size, file=f, **kwargs)
        else:
            with open(file, "rt", encoding='utf-8-sig') as f:
                yield from self.__parse_csv(batch_size, file=f, **kwargs)

    def __parse_csv(self, batch_size, file, **kwargs):
        """Read CSV in batches without loading the entire file at once."""
        csv_reader = csv.DictReader(file, **kwargs)

        cnt = 0
        batch_ = []

        for row in csv_reader:
            row["_row"] = cnt
            cnt += 1
            batch_.append(self.__clean_dict(row))

            if len(batch_) == batch_size:
                yield len(batch_), batch_
                batch_ = []

        # Yield any remaining data
        if batch_:
            yield len(batch_), batch_

    def __clean_dict(self, input_dict):
        """
        Needed in Python versions < 3.13
        Removes entries from the dictionary where:
        - The value is an empty string
        - The key is NoneType

        Args:
            input_dict (dict): The dictionary to clean.

        Returns:
            dict: A cleaned dictionary.
        """
        return {
            k: (None if isinstance(v, str) and v.strip() == "" else v)
            for k, v in input_dict.items()
            if k is not None
        }
