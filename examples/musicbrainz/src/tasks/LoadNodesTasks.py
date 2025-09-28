from etl_lib.task.data_loading.SQLLoad2Neo4jTask import SQLLoad2Neo4jTask


class LoadAreaTask(SQLLoad2Neo4jTask):
    def _sql_query(self) -> str:
        return """
               SELECT ar.id AS area_id, ar.name
               FROM area ar ORDER BY ar.id;
               """

    def _cypher_query(self) -> str:
        return """
        UNWIND $batch AS row
        MERGE (ar:Area {id: row.area_id})
            SET ar.name = row.name
        """

    def _count_query(self) -> str | None:
        return "SELECT COUNT(*) FROM area;"


class LoadAreaTypeTask(SQLLoad2Neo4jTask):
    def _sql_query(self) -> str:
        return """
               SELECT at.id   AS area_type_id,
                      at.name AS area_type_name
               FROM area_type at ORDER BY at.id;
               """

    def _cypher_query(self) -> str:
        return """
        UNWIND $batch AS row
        MERGE (t:AreaType {id: row.area_type_id})
            SET t.name = row.area_type_name
        """

    def _count_query(self) -> str | None:
        return "SELECT COUNT(*) FROM area_type;"


class LoadArtistAliasTask(SQLLoad2Neo4jTask):
    def _sql_query(self) -> str:
        return """
               SELECT aa.id        AS alias_id,
                      aa.name      AS alias,
                      aa.sort_name AS alias_sort_name,
                      aa.locale,
                      CASE
                          WHEN aa.begin_date_year IS NOT NULL AND aa.begin_date_year > 0 THEN make_date(
                                  aa.begin_date_year, COALESCE(aa.begin_date_month, 1), COALESCE(aa.begin_date_day, 1))
                          END      AS begin_date,
                      CASE
                          WHEN aa.end_date_year IS NOT NULL AND aa.end_date_year > 0 THEN make_date(aa.end_date_year,
                                                                                                    COALESCE(aa.end_date_month, 1),
                                                                                                    COALESCE(aa.end_date_day, 1))
                          END      AS end_date,
                      at.name      AS alias_type
               FROM artist_alias aa
                        LEFT JOIN artist_alias_type at
               ON aa.type = at.id
               ORDER BY aa.id;
               """

    def _cypher_query(self) -> str:
        return """
        UNWIND $batch AS row
        MERGE (aa:ArtistAlias {id: row.alias_id})
            SET aa.name = row.alias, aa.sortName = row.alias_sort_name, aa.locale = row.locale,
                aa.beginDate = row.begin_date, aa.endDate = row.end_date, aa.type = row.alias_type
        """

    def _count_query(self) -> str | None:
        return "SELECT COUNT(*) FROM artist_alias;"


class LoadArtistCreditTask(SQLLoad2Neo4jTask):
    def _sql_query(self) -> str:
        return """
               SELECT ac.id AS artist_credit_id, ac.name AS credit_name
               FROM artist_credit ac ORDER BY ac.id;
               """

    def _cypher_query(self) -> str:
        return """
        UNWIND $batch AS row
        MERGE (ac:ArtistCredit {id: row.artist_credit_id})
            SET ac.name = row.credit_name
        """

    def _count_query(self) -> str | None:
        return "SELECT COUNT(*) FROM artist_credit;"


class LoadArtistTask(SQLLoad2Neo4jTask):
    def _sql_query(self) -> str:
        return """
               SELECT a.id    AS artist_id,
                      a.name,
                      a.sort_name,
                      CASE
                          WHEN a.begin_date_year IS NOT NULL AND a.begin_date_year > 0 THEN make_date(a.begin_date_year,
                                                                                                      COALESCE(a.begin_date_month, 1),
                                                                                                      COALESCE(a.begin_date_day, 1))
                          END AS begin_date,
                      CASE
                          WHEN a.end_date_year IS NOT NULL AND a.end_date_year > 0 THEN make_date(a.end_date_year,
                                                                                                  COALESCE(a.end_date_month, 1),
                                                                                                  COALESCE(a.end_date_day, 1))
                          END AS end_date
               FROM artist a ORDER BY a.id;
               """

    def _cypher_query(self) -> str:
        return """
        UNWIND $batch AS row
        MERGE (a:Artist {id: row.artist_id})
            SET a.name = row.name, a.sortName = row.sort_name, a.beginDate = row.begin_date, a.endDate = row.end_date
        """

    def _count_query(self) -> str | None:
        return "SELECT COUNT(*) FROM artist;"


class LoadArtistTypeTask(SQLLoad2Neo4jTask):
    def _sql_query(self) -> str:
        return """
               SELECT at.id   AS artist_type_id,
                      at.name AS artist_type_name
               FROM artist_type at ORDER BY at.id;
               """

    def _cypher_query(self) -> str:
        return """
        UNWIND $batch AS row
        MERGE (at:ArtistType {id: row.artist_type_id})
            SET at.name = row.artist_type_name
        """

    def _count_query(self) -> str | None:
        return "SELECT COUNT(*) FROM artist_type;"


class LoadLabelTask(SQLLoad2Neo4jTask):
    def _sql_query(self) -> str:
        return """
               SELECT l.id    AS label_id,
                      l.name,
                      l.label_code,
                      CASE
                          WHEN l.begin_date_year > 0 THEN make_date(l.begin_date_year, COALESCE(l.begin_date_month, 1),
                                                                    COALESCE(l.begin_date_day, 1))
                          END AS begin_date,
                      CASE
                          WHEN l.end_date_year > 0 THEN make_date(l.end_date_year, COALESCE(l.end_date_month, 1),
                                                                  COALESCE(l.end_date_day, 1))
                          END AS end_date,
                      l.comment
               FROM label l ORDER BY l.id;
               """

    def _cypher_query(self) -> str:
        return """
        UNWIND $batch AS row
        MERGE (l:Label {id: row.label_id})
            SET l.name = row.name, l.labelCode = row.label_code, l.beginDate = row.begin_date, l.endDate = row.end_date, l.comment = row.comment
        """

    def _count_query(self) -> str | None:
        return "SELECT COUNT(*) FROM label;"


class LoadMediumFormatTask(SQLLoad2Neo4jTask):
    def _sql_query(self) -> str:
        return """
               SELECT mf.id   AS medium_format_id,
                      mf.name AS format_name
               FROM medium_format mf ORDER BY mf.id;
               """

    def _cypher_query(self) -> str:
        return """
        UNWIND $batch AS row
        MERGE (f:MediumFormat {id: row.medium_format_id})
            SET f.name = row.format_name
        """

    def _count_query(self) -> str | None:
        return "SELECT COUNT(*) FROM medium_format;"


class LoadMediumTask(SQLLoad2Neo4jTask):
    def _sql_query(self) -> str:
        return """
               SELECT m.id AS medium_id, m.position, m.name AS medium_name, m.track_count
               FROM medium m ORDER BY m.id;
               """

    def _cypher_query(self) -> str:
        return """
        UNWIND $batch AS row
        MERGE (m:Medium {id: row.medium_id})
            SET m.name = row.medium_name, m.position = row.position, m.trackCount = row.track_count
        """

    def _count_query(self) -> str | None:
        return "SELECT COUNT(*) FROM medium;"


class LoadRecordingTask(SQLLoad2Neo4jTask):
    def _sql_query(self) -> str:
        return """
               SELECT r.id AS recording_id, r.name AS recording_title, r.length
               FROM recording r ORDER BY r.id;"""

    def _cypher_query(self) -> str:
        return """
        UNWIND $batch AS row
        MERGE (r:Recording {id: row.recording_id})
            SET r.title = row.recording_title, r.length = row.length
        """

    def _count_query(self) -> str | None:
        return "SELECT COUNT(*) FROM recording;"


class LoadReleaseTask(SQLLoad2Neo4jTask):
    def _sql_query(self) -> str:
        return """
               SELECT r.id AS release_id, r.name AS release_title, r.barcode
               FROM release r ORDER BY r.id;
               """

    def _cypher_query(self) -> str:
        return """
        UNWIND $batch AS row
        MERGE (r:Release {id: row.release_id})
            SET r.title = row.release_title, r.barcode = row.barcode
        """

    def _count_query(self) -> str | None:
        return "SELECT COUNT(*) FROM release;"


class LoadTrackTask(SQLLoad2Neo4jTask):
    def _sql_query(self) -> str:
        return """
               SELECT t.id AS track_id, t.position, t.number, t.name AS track_name, t.length
               FROM track t ORDER BY t.id;
               """

    def _cypher_query(self) -> str:
        return """
        UNWIND $batch AS row
        MERGE (t:Track {id: row.track_id})
            SET t.name = row.track_name, t.position = row.position, t.number = row.number, t.length = row.length
        """

    def _count_query(self) -> str | None:
        return "SELECT COUNT(*) FROM track;"


class LoadWorkTask(SQLLoad2Neo4jTask):
    def _sql_query(self) -> str:
        return """
               SELECT w.id AS work_id, w.name AS work_name
               FROM work w ORDER BY w.id;"""

    def _cypher_query(self) -> str:
        return """
        UNWIND $batch AS row
        MERGE (w:Work {id: row.work_id})
            SET w.name = row.work_name
        """

    def _count_query(self) -> str | None:
        return "SELECT COUNT(*) FROM work;"


class LoadWorkTypeTask(SQLLoad2Neo4jTask):
    def _sql_query(self) -> str:
        return """
               SELECT wt.id   AS work_type_id,
                      wt.name AS work_type_name
               FROM work_type wt ORDER BY wt.id;
               """

    def _cypher_query(self) -> str:
        return """
        UNWIND $batch AS row
        MERGE (wt:WorkType {id: row.work_type_id})
            SET wt.name = row.work_type_name
        """

    def _count_query(self) -> str | None:
        return "SELECT COUNT(*) FROM work_type;"
