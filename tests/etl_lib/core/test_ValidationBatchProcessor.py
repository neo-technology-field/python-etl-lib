import json
from collections.abc import Generator
from pathlib import Path

import pytest
from pydantic import BaseModel, Field, field_validator

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

    @field_validator("field3")
    def validate_field3(cls, value):
        if not value.isalpha():
            raise ValueError("field3 must contain only alphabetic characters")
        return value


class WrapperValidationBatchProcessor(ValidationBatchProcessor):
    def __init__(self, predecessor, tmp_path: Path):
        super().__init__(DummyContext(), None, predecessor, RowModel, tmp_path / "invalid_rows.log")


def test_valid_batch(tmp_path):
    test_data = DataGenerator(
        data=[
            {
                "field1": "123",
                "field2": "45.6",
                "field3": "ValidString",
                "alias test": "True",
                "validation alias": "bar",
            }
        ]
    )
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
    test_data = DataGenerator(
        data=[
            {
                "field1": "123",
                "field2": "45.6",
                "field3": "ValidString",
                "alias test": "True",
                "validation alias": "bar",
            },
            {
                "field1": "3.14",
                "field2": "45.6",
                "field3": "ValidString",
                "alias test": "True",
                "validation alias": "bar",
            },
        ]
    )
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


class _IntegerRow(BaseModel):
    value: int


class _Batches(BatchProcessor):
    def __init__(self, batches: list[list[dict]]):
        super().__init__(None, None)
        self.batches = batches

    def get_batch(self, max_batch_size: int) -> Generator[BatchResults, None, None]:
        for rows in self.batches:
            yield BatchResults(rows, batch_size=len(rows))


def test_error_rows_accumulate_within_one_pass(tmp_path: Path) -> None:
    error_file = tmp_path / "rows.error.json"
    error_file.write_text("stale error\n", encoding="utf-8")
    processor = ValidationBatchProcessor(
        None,
        None,
        _Batches([[{"value": "bad-one"}], [{"value": "bad-two"}]]),
        _IntegerRow,
        error_file,
    )
    list(processor.get_batch(1))
    records = [json.loads(line) for line in error_file.read_text().splitlines()]
    assert [record["row"]["value"] for record in records] == ["bad-one", "bad-two"]


@pytest.mark.parametrize("second_rows", [[{"value": 1}], []])
def test_clean_second_pass_removes_stale_errors(tmp_path: Path, second_rows: list[dict]) -> None:
    error_file = tmp_path / "rows.error.json"
    source = _Batches([[{"value": "bad"}]])
    processor = ValidationBatchProcessor(None, None, source, _IntegerRow, error_file)
    list(processor.get_batch(1))
    assert error_file.exists()
    source.batches = [second_rows] if second_rows else []
    list(processor.get_batch(1))
    assert not error_file.exists()


def test_error_file_is_not_changed_until_iteration(tmp_path: Path) -> None:
    error_file = tmp_path / "rows.error.json"
    error_file.write_text("previous run\n", encoding="utf-8")
    processor = ValidationBatchProcessor(None, None, _Batches([]), _IntegerRow, error_file)
    batches = processor.get_batch(1)
    assert error_file.read_text() == "previous run\n"
    list(batches)
    assert not error_file.exists()


def test_no_model_does_not_touch_existing_error_file(tmp_path: Path) -> None:
    error_file = tmp_path / "rows.error.json"
    error_file.write_text("previous run\n", encoding="utf-8")
    processor = ValidationBatchProcessor(None, None, _Batches([[{"value": "raw"}]]), None, error_file)
    assert next(processor.get_batch(1)).chunk == [{"value": "raw"}]
    assert error_file.read_text() == "previous run\n"
