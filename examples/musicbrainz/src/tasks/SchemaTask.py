from etl_lib.task.ExecuteCypherTask import ExecuteCypherTask


class MusicBrainzSchemaTask(ExecuteCypherTask):

    def _query(self) -> str | list[str]:
        return [
            # Unique constraints
            "CREATE CONSTRAINT cons_artist_id IF NOT EXISTS FOR (a:Artist) REQUIRE a.id IS UNIQUE;",
            "CREATE CONSTRAINT cons_artistalias_id IF NOT EXISTS FOR (aa:ArtistAlias) REQUIRE aa.id IS UNIQUE;",
            "CREATE CONSTRAINT cons_artistcredit_id IF NOT EXISTS FOR (ac:ArtistCredit) REQUIRE ac.id IS UNIQUE;",
            "CREATE CONSTRAINT cons_track_id IF NOT EXISTS FOR (t:Track) REQUIRE t.id IS UNIQUE;",
            "CREATE CONSTRAINT cons_medium_id IF NOT EXISTS FOR (m:Medium) REQUIRE m.id IS UNIQUE;",
            "CREATE CONSTRAINT cons_release_id IF NOT EXISTS FOR (r:Release) REQUIRE r.id IS UNIQUE;",
            "CREATE CONSTRAINT cons_label_id IF NOT EXISTS FOR (l:Label) REQUIRE l.id IS UNIQUE;",
            "CREATE CONSTRAINT cons_recording_id IF NOT EXISTS FOR (rec:Recording) REQUIRE rec.id IS UNIQUE;",
            "CREATE CONSTRAINT cons_work_id IF NOT EXISTS FOR (w:Work) REQUIRE w.id IS UNIQUE;",
            "CREATE CONSTRAINT cons_area_id IF NOT EXISTS FOR (ar:Area) REQUIRE ar.id IS UNIQUE;",
            "CREATE CONSTRAINT cons_artisttype_id IF NOT EXISTS FOR (at:ArtistType) REQUIRE at.id IS UNIQUE;",
            "CREATE CONSTRAINT cons_areatype_id IF NOT EXISTS FOR (at:AreaType) REQUIRE at.id IS UNIQUE;",
            "CREATE CONSTRAINT cons_worktype_id IF NOT EXISTS FOR (wt:WorkType) REQUIRE wt.id IS UNIQUE;",
            "CREATE CONSTRAINT cons_mediumformat_id IF NOT EXISTS FOR (mf:MediumFormat) REQUIRE mf.id IS UNIQUE;",

            # Indexes on common lookup or query fields
            "CREATE INDEX idx_artist_name IF NOT EXISTS FOR (a:Artist) ON (a.name);",
            "CREATE INDEX idx_artist_sortname IF NOT EXISTS FOR (a:Artist) ON (a.sortName);",
            "CREATE INDEX idx_artistalias_name IF NOT EXISTS FOR (aa:ArtistAlias) ON (aa.name);",
            "CREATE INDEX idx_artistcredit_name IF NOT EXISTS FOR (ac:ArtistCredit) ON (ac.name);",
            "CREATE INDEX idx_track_name IF NOT EXISTS FOR (t:Track) ON (t.name);",
            "CREATE INDEX idx_medium_name IF NOT EXISTS FOR (m:Medium) ON (m.name);",
            "CREATE INDEX idx_release_title IF NOT EXISTS FOR (r:Release) ON (r.title);",
            "CREATE INDEX idx_label_name IF NOT EXISTS FOR (l:Label) ON (l.name);",
            "CREATE INDEX idx_recording_title IF NOT EXISTS FOR (rec:Recording) ON (rec.title);",
            "CREATE INDEX idx_work_name IF NOT EXISTS FOR (w:Work) ON (w.name);",
            "CREATE INDEX idx_area_name IF NOT EXISTS FOR (ar:Area) ON (ar.name);",
            "CREATE INDEX idx_artisttype_name IF NOT EXISTS FOR (at:ArtistType) ON (at.name);",
            "CREATE INDEX idx_areatype_name IF NOT EXISTS FOR (at:AreaType) ON (at.name);",
            "CREATE INDEX idx_worktype_name IF NOT EXISTS FOR (wt:WorkType) ON (wt.name);",
            "CREATE INDEX idx_mediumformat_name IF NOT EXISTS FOR (mf:MediumFormat) ON (mf.name);"
        ]
