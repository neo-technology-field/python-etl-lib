from pydantic import BaseModel, computed_field

from etl_lib.ETLContext import ETLContext
from etl_lib.task.data_loading.CSVLoad2Neo4jTask import CSVLoad2Neo4jTasks


class LoadCalendarTask(CSVLoad2Neo4jTasks):
    class Calendar(BaseModel):
        service_id: str
        monday: bool
        tuesday: bool
        wednesday: bool
        thursday: bool
        friday: bool
        saturday: bool
        sunday: bool

        @computed_field
        @property
        def labels(self) -> list[str]:
            label_array = []
            if self.monday:
                label_array.append("RUNS_1")
            if self.tuesday:
                label_array.append("RUNS_2")
            if self.wednesday:
                label_array.append("RUNS_3")
            if self.thursday:
                label_array.append("RUNS_4")
            if self.friday:
                label_array.append("RUNS_5")
            if self.saturday:
                label_array.append("RUNS_6")
            if self.sunday:
                label_array.append("RUNS_7")
            return label_array

    def __init__(self, context: ETLContext):
        super().__init__(context, LoadCalendarTask.Calendar)

    def _query(self):
        return """UNWIND $batch as row
        MATCH (t:Trip {serviceId: row.service_id})
        WITH t, row UNWIND row.labels AS label 
            SET t:$(label)
        """

    @classmethod
    def file_name(cls):
        return "calendar.txt"
