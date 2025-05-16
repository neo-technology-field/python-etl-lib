from etl_lib.task.data_loading.SQLLoad2Neo4jTask import SQLLoad2Neo4jTask


class CreateArtistCreditRelTask(SQLLoad2Neo4jTask):
    def _sql_query(self) -> str:
        return """
               SELECT acn.artist        AS artist_id,
                      acn.artist_credit AS artist_credit_id
               FROM artist_credit_name AS acn;
               """

    def _cypher_query(self) -> str:
        return """
        UNWIND $batch AS row
        MATCH (a:Artist {id: row.artist_id})
        MATCH (ac:ArtistCredit {id: row.artist_credit_id})
        MERGE (a)-[:CREDITED_AS]->(ac)
        """

    def _count_query(self) -> str | None:
        return "SELECT COUNT(*) FROM artist_credit_name;"


class CreateCreditOnTrackRelTask(SQLLoad2Neo4jTask):
    def _sql_query(self) -> str:
        return """
               SELECT t.artist_credit AS artist_credit_id,
                      t.id            AS track_id
               FROM track t;
               """

    def _cypher_query(self) -> str:
        return """
        UNWIND $batch AS row
        MATCH (ac:ArtistCredit {id: row.artist_credit_id})
        MATCH (t:Track {id: row.track_id})
        MERGE (ac)-[:CREDITED_ON]->(t)
        """

    def _count_query(self) -> str | None:
        return "SELECT COUNT(*) FROM track WHERE artist_credit IS NOT NULL;"


class CreateTrackToMediumRelTask(SQLLoad2Neo4jTask):
    def _sql_query(self) -> str:
        return """
               SELECT t.id     AS track_id,
                      t.medium AS medium_id
               FROM track t;
               """

    def _cypher_query(self) -> str:
        return """
        UNWIND $batch AS row
        MATCH (t:Track {id: row.track_id})
        MATCH (m:Medium {id: row.medium_id})
        MERGE (t)-[:APPEARS_ON]->(m)
        """

    def _count_query(self) -> str | None:
        return "SELECT COUNT(*) FROM track WHERE medium IS NOT NULL;"


class CreateMediumToReleaseRelTask(SQLLoad2Neo4jTask):
    def _sql_query(self) -> str:
        return """
               SELECT m.id AS medium_id,
                      r.id AS release_id
               FROM medium m
                        JOIN release r ON m.release = r.id;
               """

    def _cypher_query(self) -> str:
        return """
        UNWIND $batch AS row
        MATCH (m:Medium {id: row.medium_id})
        MATCH (r:Release {id: row.release_id})
        MERGE (m)<-[:RELEASED_ON]-(r)
        """

    def _count_query(self) -> str | None:
        return "SELECT COUNT(*) FROM medium WHERE release IS NOT NULL;"


class CreateReleaseToLabelRelTask(SQLLoad2Neo4jTask):
    def _sql_query(self) -> str:
        return """
               SELECT rl.release AS release_id,
                      rl.label   AS label_id
               FROM release_label rl;
               """

    def _cypher_query(self) -> str:
        return """
        UNWIND $batch AS row
        MATCH (r:Release {id: row.release_id})
        MATCH (l:Label {id: row.label_id})
        MERGE (r)-[:RELEASED_ON]->(l)
        """

    def _count_query(self) -> str | None:
        return "SELECT COUNT(*) FROM release_label;"


class CreateTrackToRecordingRelTask(SQLLoad2Neo4jTask):
    def _sql_query(self) -> str:
        return """
               SELECT t.id        AS track_id,
                      t.recording AS recording_id
               FROM track t;
               """

    def _cypher_query(self) -> str:
        return """
        UNWIND $batch AS row
        MATCH (t:Track {id: row.track_id})
        MATCH (r:Recording {id: row.recording_id})
        MERGE (t)-[:IS_RECORDING]->(r)
        """

    def _count_query(self) -> str | None:
        return "SELECT COUNT(*) FROM track WHERE recording IS NOT NULL;"


class CreateRecordingToWorkRelTask(SQLLoad2Neo4jTask):
    def _sql_query(self) -> str:
        return """
               SELECT lrw.entity0 AS recording_id,
                      lrw.entity1 AS work_id
               FROM l_recording_work lrw;
               """

    def _cypher_query(self) -> str:
        return """
        UNWIND $batch AS row
        MATCH (r:Recording {id: row.recording_id})
        MATCH (w:Work {id: row.work_id})
        MERGE (r)-[:PERFORMANCE]->(w)
        """

    def _count_query(self) -> str | None:
        return "SELECT COUNT(*) FROM l_recording_work lrw JOIN link l ON lrw.link = l.id JOIN link_type lt ON l.link_type = lt.id WHERE lt.name = 'performance';"


class CreateArtistToAreaRelTask(SQLLoad2Neo4jTask):
    def _sql_query(self) -> str:
        return """
               SELECT a.id   AS artist_id,
                      a.area AS area_id
               FROM artist a
               WHERE a.area IS NOT NULL;
               """

    def _cypher_query(self) -> str:
        return """
        UNWIND $batch AS row
        MATCH (a:Artist {id: row.artist_id})
        MATCH (ar:Area {id: row.area_id})
        MERGE (a)-[:FROM_AREA]->(ar)
        """

    def _count_query(self) -> str | None:
        return "SELECT COUNT(*) FROM artist WHERE area IS NOT NULL;"


class CreateArtistAliasRelTask(SQLLoad2Neo4jTask):
    def _sql_query(self) -> str:
        return """
               SELECT a.id  AS artist_id,
                      aa.id AS alias_id
               FROM artist AS a
                        JOIN artist_alias AS aa
                             ON a.id = aa.artist;
               """

    def _cypher_query(self) -> str:
        return """
        UNWIND $batch AS row
        MATCH (a:Artist {id: row.artist_id})
        MATCH (aa:ArtistAlias {id: row.alias_id})
        MERGE (a)-[:HAS_ALIAS]->(aa)
        """

    def _count_query(self) -> str | None:
        return "SELECT COUNT(*) FROM artist_alias;"
