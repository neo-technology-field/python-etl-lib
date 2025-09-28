import json
from pathlib import Path
from typing import Type, Generator

from pydantic import BaseModel, ValidationError

from etl_lib.core.BatchProcessor import BatchProcessor, BatchResults
from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.Task import Task
from etl_lib.core.utils import merge_summery


class ValidationBatchProcessor(BatchProcessor):
    """
    Batch processor for validation, using Pydantic.
    """

    def __init__(self, context: ETLContext, task: Task, predecessor, model: Type[BaseModel], error_file: Path):
        """
        Constructs a new ValidationBatchProcessor.

        The :py:class:`etl_lib.core.BatchProcessor.BatchResults` returned from the :py:func:`~get_batch` of this
        implementation will contain the following additional entries:

        - `valid_rows`: Number of valid rows.
        - `invalid_rows`: Number of invalid rows.

        Args:
            context: :py:class:`etl_lib.core.ETLContext.ETLContext` instance.
            task: :py:class:`etl_lib.core.Task.Task` instance owning this batchProcessor.
            predecessor: BatchProcessor which :py:func:`~get_batch` function will be called to receive batches to process.
            model: Pydantic model class used to validate each row in the batch.
            error_file: Path to the file that will receive each row that did not pass validation.
                Each row in this file will contain the original data together with all validation errors for this row.
        """
        super().__init__(context, task, predecessor)
        if model is not None and error_file is None:
            raise ValueError('you must provide error file if the model is specified')
        self.error_file = error_file
        self.model = model

    def get_batch(self, max_batch__size: int) -> Generator[BatchResults, None, None]:
        assert self.predecessor is not None

        for batch in self.predecessor.get_batch(max_batch__size):
            valid_rows = []
            invalid_rows = []

            for row in batch.chunk:
                try:
                    # Validate and transform the row
                    validated_row = json.loads(self.model(**row).model_dump_json())
                    valid_rows.append(validated_row)
                except ValidationError as e:
                    # Collect invalid rows with errors
                    invalid_rows.append({"row": row, "errors": e.errors()})

            # Write invalid rows to the error file
            if invalid_rows:
                with open(self.error_file, "a") as f:
                    for invalid in invalid_rows:
                        # the following is needed as ValueError (contained in 'ctx') is not json serializable
                        serializable = {"row": invalid["row"],
                                        "errors": [{k: v for k, v in e.items() if k != "ctx"} for e in
                                                   invalid["errors"]]}
                        f.write(f"{json.dumps(serializable)}\n")

            # Yield BatchResults with statistics
            yield BatchResults(
                chunk=valid_rows,
                statistics=merge_summery(batch.statistics, {
                    "valid_rows": len(valid_rows),
                    "invalid_rows": len(invalid_rows)
                }),
                batch_size=len(batch.chunk)
            )
