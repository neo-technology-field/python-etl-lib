from etl_lib.task.SchemaTask import SchemaTask as BaseSchemaTask


class SchemaTask(BaseSchemaTask):

    def _schema_statements(self) -> list[str]:
        return [
            "CREATE CONSTRAINT cons_agency_id IF NOT EXISTS FOR (a:Agency) REQUIRE a.id IS UNIQUE;",
            "CREATE CONSTRAINT cons_route_id IF NOT EXISTS FOR (r:Route) REQUIRE r.id IS UNIQUE;",
            "CREATE CONSTRAINT cons_trip_id IF NOT EXISTS FOR (t:Trip) REQUIRE t.id IS UNIQUE;",
            "CREATE CONSTRAINT cons_stop_id IF NOT EXISTS FOR (s:Stop) REQUIRE s.id IS UNIQUE;",
            "CREATE INDEX idx_trip_service IF NOT EXISTS FOR (t:Trip) ON (t.serviceId);",
            "CREATE INDEX idx_stoptime_seq IF NOT EXISTS FOR (st:StopTime) ON (st.stopSequence);",
            "CREATE INDEX idx_stop_name IF NOT EXISTS FOR (s:Stop) ON (s.name);"
        ]
