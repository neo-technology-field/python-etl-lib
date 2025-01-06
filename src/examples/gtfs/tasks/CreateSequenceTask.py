from etl_lib.task.ExecuteCypherTask import ExecuteCypherTask


class CreateSequenceTask(ExecuteCypherTask):

    def _query(self) -> str | list[str]:
        return """
        call apoc.periodic.iterate('match (t:Trip) return t',
        'match (t)<-[:BELONGS_TO]-(st) with st order by st.stopSequence asc
        with collect(st) as stops
        unwind range(0, size(stops)-2) as i
        with stops[i] as curr, stops[i+1] as next
        merge (curr)-[:NEXT_STOP]->(next)', {batchmode: "BATCH", parallel:true, parallel:true, batchSize:1});
        """
