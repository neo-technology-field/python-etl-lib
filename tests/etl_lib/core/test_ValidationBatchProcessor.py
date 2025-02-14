from pathlib import Path
from typing import Generator

import json

from pydantic import BaseModel, field_validator, Field

from etl_lib.core.BatchProcessor import BatchProcessor, BatchResults
from etl_lib.core.ValidationBatchProcessor import ValidationBatchProcessor
from etl_lib.test_utils.utils import DummyContext


class DataGenerator(BatchProcessor):

    def __init__(self, data: [{}]):
        super().__init__(None, None)
        self.data = data

    def get_batch(self, batch_size: int) -> Generator[BatchResults, None, None]:
        yield BatchResults(self.data)


# Define a Pydantic model for validation
class RowModel(BaseModel):
    field1: int
    field2: float
    field3: str
    alias_test: bool = Field(alias="alias test", default=None)
    validation_alias: str = Field(alias="validation alias", default=None)

    @field_validator('field3')
    def validate_field3(cls, value):
        if not value.isalpha():
            raise ValueError("field3 must contain only alphabetic characters")
        return value


class WrapperValidationBatchProcessor(ValidationBatchProcessor):

    def __init__(self, predecessor, tmp_path: Path):
        super().__init__(DummyContext(), None,predecessor, RowModel, tmp_path / "invalid_rows.log")


def test_valid_batch(tmp_path):
    test_data = DataGenerator(data=[
        {"field1": "123", "field2": "45.6", "field3": "ValidString", "alias test": "True", "validation alias": "bar"}])
    processor = WrapperValidationBatchProcessor(test_data, tmp_path)

    result = next(processor.get_batch(1))

    assert result.statistics["valid_rows"] == 1
    assert result.statistics["invalid_rows"] == 0
    assert len(result.chunk) == 1
    assert result.chunk[0]["alias_test"] == True
    assert result.chunk[0]["validation_alias"] == "bar"
    assert result.chunk[0]["field1"] == 123
    assert result.chunk[0]["field2"] == 45.6

def test_invalid_batch(tmp_path):
    test_data = DataGenerator(data=
    [
        {"field1": "123", "field2": "45.6", "field3": "ValidString", "alias test": "True", "validation alias": "bar"},
        {"field1": "3.14", "field2": "45.6", "field3": "ValidString", "alias test": "True", "validation alias": "bar"}
    ])
    processor = WrapperValidationBatchProcessor(test_data, tmp_path)

    result = next(processor.get_batch(1))

    assert result.statistics["valid_rows"] == 1
    assert result.statistics["invalid_rows"] == 1
    assert len(result.chunk) == 1
    assert result.chunk[0]["alias_test"] == True
    assert result.chunk[0]["validation_alias"] == "bar"
    assert result.chunk[0]["field1"] == 123
    assert result.chunk[0]["field2"] == 45.6

    error_file = tmp_path / "invalid_rows.log"
    assert error_file.exists()
    errors = []
    with open(error_file) as f:
        for line in f:
            errors.append(json.loads(line))
    assert len(errors) == 1
    assert errors[0]["errors"][0]["type"] == "int_parsing"
