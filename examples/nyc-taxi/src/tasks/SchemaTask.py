from etl_lib.core.ETLContext import ETLContext
from etl_lib.task.ExecuteCypherTask import ExecuteCypherTask


class SchemaTask(ExecuteCypherTask):
    """Create constraints for Location and Vendor."""

    def __init__(self, context: ETLContext):
        super().__init__(context)

    def _query(self):
        return [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (l:Location) REQUIRE l.id IS NODE KEY",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Trip) REQUIRE t.id IS NODE KEY",
            "CREATE INDEX IF NOT EXISTS FOR (t:Trip) ON (t.vendor)"
        ]
