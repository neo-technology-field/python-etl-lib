import json
from pathlib import Path
from typing import Type, Generator

from etl_lib.core.BatchProcessor import BatchProcessor, BatchResults
from pydantic import BaseModel, ValidationError

from etl_lib.core.Task import merge_summery


class ValidationBatchProcessor(BatchProcessor):

    def __init__(self, predecessor, model: Type[BaseModel], error_file: Path):
        super().__init__(predecessor)
        self.error_file = error_file
        self.model = model

    def get_batch(self, batch_size: int) -> Generator[BatchResults, None, None]:
        assert self.predecessor is not None

        for batch in self.predecessor.get_batch(batch_size):
            valid_rows = []
            invalid_rows = []

            for row in batch.chunk:
                try:
                    # Validate and transform the row
                    validated_row = self.model(**row).model_dump()
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
                statistics=merge_summery(batch.statistics,{
                    "valid_count": len(valid_rows),
                    "invalid_count": len(invalid_rows),
                    "batch_size": len(batch.chunk)
                }),
                batch_size=len(batch.chunk)
            )
