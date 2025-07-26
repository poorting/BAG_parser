import duckdb
import polars as pl
import os

import utils
import config


class DatabaseDuckdb:
    connection = None

    # cursor = None
    schema_overrides = {
        'gemeente_id': pl.datatypes.UInt64,
        'woonplaats_id': pl.datatypes.UInt64,
        'nummer_id': pl.datatypes.String,
        'pand_id': pl.datatypes.String,
        'id': pl.datatypes.String,
        'pos': pl.datatypes.String,
        'begindatum_geldigheid': pl.datatypes.String,
        'einddatum_geldigheid': pl.datatypes.String,
        'verkorte_naam': pl.datatypes.String,
        'naam': pl.datatypes.String,
        'huisletter': pl.datatypes.String,
        'toevoeging': pl.datatypes.String,
        'yadayada': pl.datatypes.String,
    }

    def __init__(self):
        self.connection = duckdb.connect(config.file_db_duckdb)
        # self.connection = duckdb.connect()
        # install and load extensions
        self.connection.execute(f"""
            install spatial;
            load spatial;
            install json;
            load json;
        """)

    def close(self):
        self.connection.close()

    def fetchone(self, sql):
        return self.connection.execute(sql).fetchone()[0]

    def fetchall(self, sql):
        return self.connection.execute(sql).fetchall()

    def fetchmany(self, size=1000):
        return self.connection.fetchmany(size)

    def post_process(self, sql):
        self.connection.execute(sql)

    def enable_progress_bar(self):
        self.connection.execute("PRAGMA enable_progress_bar;")

    def disable_progress_bar(self):
        self.connection.execute("PRAGMA disable_progress_bar;")

    def copy_database(self, target_db_path):
        # Remove target database if it exists
        if os.path.isfile(target_db_path) or os.path.islink(target_db_path):
            os.unlink(target_db_path)

        # Attach the new database
        self.connection.execute(f"ATTACH '{target_db_path}'")
        # The name of the db in duckdb
        db_name = os.path.splitext(os.path.basename(target_db_path))[0]

        # Copy the database
        self.connection.execute(f"COPY FROM DATABASE bag TO {db_name}")
        self.connection.execute(f"DETACH {db_name};")

    def save_woonplaats(self, datarows):
        df = pl.from_dicts(datarows, schema_overrides=self.schema_overrides, infer_schema_length=None)
        geom = "st_geomfromgeojson(geometry::json) as geometry" if config.parse_geometries else "NULL as geometry"

        self.connection.execute(
            f"""INSERT INTO woonplaatsen (woonplaats_id, naam, geometry, status, begindatum_geldigheid, einddatum_geldigheid) select
            id as woonplaatsen_id,
            naam,
            -- geometry,
            {geom},
            status,
            begindatum_geldigheid,
            einddatum_geldigheid
            FROM df""")

    def save_woonplaats_geometry(self, woonplaatsen):
        self.connection.executemany(
            "UPDATE woonplaatsen SET geometry=? WHERE id=?;",
            woonplaatsen)

    def save_pand_geometry(self, panden):
        self.connection.executemany(
            "UPDATE panden SET geometry=? WHERE id=?;",
            panden)

    def save_lon_lat(self, table_name, records):
        self.connection.executemany(
            f"UPDATE {table_name} SET longitude=?, latitude=? WHERE id=?;",
            records)

    def create_gemeenten_provincies(self, file_gemeenten):
        try:
            if file_gemeenten.endswith('.xlsx'):
                self.connection.execute(f"""
                    CREATE OR REPLACE TEMP TABLE gem_prov_read as select * from read_xlsx('{file_gemeenten}', sheet='Gemeenten_alfabetisch');
                """)
            else:
                self.connection.execute(f"""
                CREATE OR REPLACE TEMP TABLE gem_prov_read as select * from '{file_gemeenten}';
            """)

            self.connection.execute(f"""
                CREATE OR REPLACE TABLE provincies as select Provinciecode::UBIGINT as id, ProvinciecodePV as pv_code, first(Provincienaam) as naam FROM gem_prov_read group by all;
            """)
            self.connection.execute(f"""
                CREATE OR REPLACE TABLE gemeenten as select Gemeentecode::UBIGINT as id, GemeentecodeGM as gm_code, Gemeentenaam as naam, Provinciecode::UBIGINT as provincie_id FROM gem_prov_read group by all;
            """)
        except Exception as e:
            utils.print_log(str(e), error=True)

    def save_gemeente_woonplaats(self, datarows):
        df = pl.from_dicts(datarows, schema_overrides=self.schema_overrides, infer_schema_length=None)
        df.with_columns(
            pl.when(pl.col(pl.String).str.len_chars() == 0)
            .then(None)
            .otherwise(pl.col(pl.String))
            .name.keep()
        )
        # print(df)
        try:
            self.connection.execute("INSERT INTO gemeente_woonplaatsen SELECT "
                                    "gemeente_id,"
                                    "woonplaats_id,"
                                    "status,"
                                    "begindatum_geldigheid,"
                                    "einddatum_geldigheid"
                                    " FROM df")
        except Exception as e:
            print(e, flush=True)
            # print(df.dtypes, flush=True)

    def add_gemeenten_to_woonplaatsen(self):
        self.connection.execute(
            """UPDATE woonplaatsen SET gemeente_id=gw.gemeente_id
            FROM (SELECT gemeente_id, woonplaats_id FROM gemeente_woonplaatsen) AS gw
            WHERE gw.woonplaats_id = woonplaatsen.woonplaats_id
            """)

    def save_openbare_ruimte(self, datarows):
        try:
            df = pl.from_dicts(datarows, schema_overrides=self.schema_overrides, infer_schema_length=None)
            df.with_columns(
                pl.when(pl.col(pl.String).str.len_chars() == 0)
                .then(None)
                .otherwise(pl.col(pl.String))
                .name.keep()
            )

            self.connection.execute("INSERT OR REPLACE INTO openbare_ruimten SELECT "
                                    "id, "
                                    "naam, "
                                    # "lange_naam, "
                                    "verkorte_naam, "
                                    "type, "
                                    "woonplaats_id,"
                                    "status,"
                                    "begindatum_geldigheid,"
                                    "einddatum_geldigheid"
                                    " FROM df ORDER BY id ASC")
        except pl.exceptions.ComputeError as e:
            utils.print_log(str(e), error=True)
        except Exception as e:
            utils.print_log(str(e), error=True)

    def save_nummer(self, datarows):
        df = pl.from_dicts(datarows, schema_overrides=self.schema_overrides, infer_schema_length=None)
        df.with_columns(
            pl.when(pl.col(pl.String).str.len_chars() == 0)
            .then(None)
            .otherwise(pl.col(pl.String))
            .name.keep()
        )
        try:
            self.connection.execute("INSERT OR REPLACE INTO nummers SELECT "
                                    "id,postcode,huisnummer,"
                                    "huisletter,"
                                    "toevoeging,"
                                    "woonplaats_id,"
                                    "openbare_ruimte_id,"
                                    "status,"
                                    "begindatum_geldigheid,"
                                    "einddatum_geldigheid"
                                    " FROM df ORDER BY id ASC")
        except Exception as e:
            print(e, flush=True)
            # print(df.dtypes, flush=True)

    def save_pand(self, datarows):
        try:
            df = pl.from_dicts(datarows, schema_overrides=self.schema_overrides, infer_schema_length=None)
            df.with_columns(
                pl.when(pl.col(pl.String).str.len_chars() == 0)
                .then(None)
                .otherwise(pl.col(pl.String))
                .name.keep()
            )

            geom = "st_geomfromgeojson(geometry::json)" if config.parse_geometries else "NULL"
            self.connection.execute("INSERT OR REPLACE INTO panden SELECT "
                                    "id, bouwjaar, "
                                    f"{geom} as geometry,"
                                    # "geometry,"
                                    "status,"
                                    "begindatum_geldigheid,"
                                    "einddatum_geldigheid"
                                    " FROM df ORDER BY id ASC")
        except Exception as e:
            utils.print_log(str(e), error=True)

    def save_verblijfsobject(self, datarows):
        try:
            df = pl.from_dicts(datarows, schema_overrides=self.schema_overrides, infer_schema_length=None)
            df.with_columns(
                pl.when(pl.col(pl.String).str.len_chars() == 0)
                .then(None)
                .otherwise(pl.col(pl.String))
                .name.keep()
            )

            self.connection.execute("INSERT OR REPLACE INTO verblijfsobjecten SELECT "
                                    "id,nummer_id,pand_id,"
                                    "try_cast(oppervlakte as double) as oppervlakte,"
                                    "try_cast(rd_x as double) as rd_x ,"
                                    "try_cast(rd_y as double) as rd_y,"
                                    "try_cast(latitude as double) as latitude,"
                                    "try_cast(longitude as double) as longitude,"
                                    "NULL as lon_lat,"
                                    "gebruiksdoel,"
                                    "nevenadressen,"
                                    "status,"
                                    "begindatum_geldigheid,"
                                    "einddatum_geldigheid"
                                    " FROM df ORDER BY nummer_id ASC")
        except Exception as e:
            utils.print_log(str(e), error=True)

    def save_ligplaats(self, datarows):
        try:
            df = pl.from_dicts(datarows, schema_overrides=self.schema_overrides, infer_schema_length=None)
            df.with_columns(
                pl.when(pl.col(pl.String).str.len_chars() == 0)
                .then(None)
                .otherwise(pl.col(pl.String))
                .name.keep()
            )

            geom = "st_geomfromgeojson(geometry::json)" if config.parse_geometries else "NULL"
            self.connection.execute("INSERT OR REPLACE INTO ligplaatsen SELECT "
                                    "id,nummer_id,"
                                    "try_cast(rd_x as double) as rd_x ,"
                                    "try_cast(rd_y as double) as rd_y,"
                                    "try_cast(latitude as double) as latitude,"
                                    "try_cast(longitude as double) as longitude,"
                                    "NULL as lon_lat,"
                                    f"{geom} as geometry,"
                                    "status,"
                                    "begindatum_geldigheid,"
                                    "einddatum_geldigheid"
                                    " FROM df ORDER BY nummer_id ASC")
        except Exception as e:
            utils.print_log(str(e), error=True)

    def save_standplaats(self, datarows):
        try:
            df = pl.from_dicts(datarows, schema_overrides=self.schema_overrides, infer_schema_length=None)
            df.with_columns(
                pl.when(pl.col(pl.String).str.len_chars() == 0)
                .then(None)
                .otherwise(pl.col(pl.String))
                .name.keep()
            )

            geom = "st_geomfromgeojson(geometry::json)" if config.parse_geometries else "NULL"
            self.connection.execute("INSERT OR REPLACE INTO standplaatsen SELECT "
                                    "id,nummer_id,"
                                    "try_cast(rd_x as double) as rd_x ,"
                                    "try_cast(rd_y as double) as rd_y,"
                                    "try_cast(latitude as double) as latitude,"
                                    "try_cast(longitude as double) as longitude,"
                                    "NULL as lon_lat,"
                                    f"{geom} as geometry,"
                                    "status,"
                                    "begindatum_geldigheid,"
                                    "einddatum_geldigheid"
                                    " FROM df ORDER BY nummer_id ASC")
        except Exception as e:
            utils.print_log(str(e), error=True)

    def create_bag_tables(self):
        self.connection.execute("""
            DROP TABLE IF EXISTS woonplaatsen;
            CREATE OR REPLACE SEQUENCE seq_wpid START 1;
            CREATE TABLE woonplaatsen (
                id UBIGINT PRIMARY KEY DEFAULT NEXTVAL('seq_wpid'),
                woonplaats_id UBIGINT,
                naam TEXT,
                gemeente_id UBIGINT,
                geometry GEOMETRY,
                status TEXT,
                begindatum_geldigheid TEXT,
                einddatum_geldigheid TEXT);
            
            DROP TABLE IF EXISTS gemeente_woonplaatsen;
            CREATE TABLE gemeente_woonplaatsen (
                gemeente_id UBIGINT,
                woonplaats_id UBIGINT,
                status TEXT, 
                begindatum_geldigheid DATE, 
                einddatum_geldigheid DATE
            );              

            DROP TABLE IF EXISTS openbare_ruimten;
            CREATE TABLE openbare_ruimten (
                id UBIGINT PRIMARY KEY,
                naam TEXT,
                -- lange_naam TEXT,
                verkorte_naam TEXT, 
                type TEXT,
                woonplaats_id UBIGINT,
                status TEXT,
                begindatum_geldigheid DATE,
                einddatum_geldigheid DATE);

            DROP TABLE IF EXISTS nummers;
            CREATE TABLE nummers (
                id TEXT PRIMARY KEY, 
                postcode TEXT, 
                huisnummer INTEGER, 
                huisletter TEXT,
                toevoeging TEXT, 
                woonplaats_id UBIGINT, 
                openbare_ruimte_id UBIGINT,
                status TEXT, 
                begindatum_geldigheid DATE, 
                einddatum_geldigheid DATE);

            DROP TABLE IF EXISTS panden;
            CREATE TABLE panden (id TEXT PRIMARY KEY, 
                bouwjaar INTEGER, 
                geometry GEOMETRY,
                status TEXT, 
                begindatum_geldigheid DATE, 
                einddatum_geldigheid DATE);

            DROP TABLE IF EXISTS verblijfsobjecten;
            CREATE TABLE verblijfsobjecten (
                id TEXT PRIMARY KEY, 
                nummer_id TEXT, 
                pand_id TEXT, 
                oppervlakte DOUBLE, 
                rd_x DOUBLE, 
                rd_y DOUBLE, 
                latitude DOUBLE, 
                longitude DOUBLE, 
                lon_lat GEOMETRY, 
                gebruiksdoel TEXT, 
                nevenadressen TEXT,
                status TEXT, 
                begindatum_geldigheid DATE, 
                einddatum_geldigheid DATE);           

            DROP TABLE IF EXISTS ligplaatsen;
            CREATE TABLE ligplaatsen (
                id TEXT PRIMARY KEY, 
                nummer_id TEXT, 
                rd_x DOUBLE, 
                rd_y DOUBLE, 
                latitude DOUBLE, 
                longitude DOUBLE,
                lon_lat GEOMETRY, 
                geometry GEOMETRY, 
                status TEXT, 
                begindatum_geldigheid DATE, 
                einddatum_geldigheid DATE);              

            DROP TABLE IF EXISTS standplaatsen;
            CREATE TABLE standplaatsen (
                id TEXT PRIMARY KEY, 
                nummer_id TEXT, 
                rd_x DOUBLE, 
                rd_y DOUBLE, 
                latitude DOUBLE, 
                longitude DOUBLE, 
                lon_lat GEOMETRY, 
                geometry GEOMETRY, 
                status TEXT, 
                begindatum_geldigheid DATE, 
                einddatum_geldigheid DATE);              
        """)

    def create_adressen_from_bag(self):

        utils.print_log('create adressen tabel: import adressen')

        # Use CTAS (Create Table As Select) since it is ~ 30% faster
        # than creating a table first and then inserting values
        # A primary key column cannot be created that way,
        # So that is done afterwards by altering the nummer_id column.
        self.connection.execute("""
            CREATE OR REPLACE TABLE adressen AS
            SELECT
                n.id AS nummer_id,
                n.begindatum_geldigheid as nummer_begindatum_geldigheid,
                n.einddatum_geldigheid as nummer_einddatum_geldigheid,
                split(p.id, e'\t') as pand_id,
                p.begindatum_geldigheid as pand_begindatum_geldigheid,
                p.einddatum_geldigheid as pand_einddatum_geldigheid,
                v.id AS verblijfsobject_id,
                w.gemeente_id as gemeente_id,
                o.woonplaats_id as woonplaats_id,
                o.id as openbare_ruimte_id,
                'verblijfsobject' as object_type,
                split(v.gebruiksdoel,e'\t') as gebruiksdoel,
                n.postcode as postcode,
                n.huisnummer as huisnummer,
                n.huisletter as huisletter,
                n.toevoeging as toevoeging,
                v.oppervlakte as oppervlakte,
                v.rd_x as rd_x,
                v.rd_y as rd_y,
                v.longitude as longitude,
                v.latitude as latitude,
                v.lon_lat,
                p.bouwjaar,
                NULL::TEXT as hoofd_nummer_id,
                p.geometry
            FROM nummers n
            LEFT JOIN openbare_ruimten o  ON o.id            = n.openbare_ruimte_id
            LEFT JOIN woonplaatsen w      ON w.woonplaats_id = o.woonplaats_id
            LEFT JOIN verblijfsobjecten v ON v.nummer_id     = n.id
            LEFT JOIN panden p            ON v.pand_id       = p.id;
        """)

        utils.print_log('create adressen tabel: set primary key')
        self.connection.execute("""
            ALTER TABLE adressen ADD PRIMARY KEY (nummer_id);
        """)

        utils.print_log('create adressen tabel: importeer pand info voor adressen met meerdere panden')
        self.adressen_import_meerdere_panden()

        utils.print_log('create adressen tabel: import ligplaatsen data')
        self.adressen_import_ligplaatsen()

        utils.print_log('create adressen tabel: import standplaatsen data')
        self.adressen_import_standplaatsen()

        utils.print_log('create adressen tabel: import woonplaatsen from nummers')
        self.adressen_update_woonplaatsen_from_nummers()

        utils.print_log('create adressen tabel: update nevenadressen data')
        self.adressen_update_nevenadressen()

        utils.print_log('create adressen tabel: fill lon_lat column from longitude/latitude')
        self.connection.execute(
            "UPDATE adressen SET lon_lat=st_point(longitude, latitude) WHERE lon_lat is NULL and longitude is not NULL and latitude is not NULL")

        # Creating R-Tree index disabled as it can slow down specific queries significantly...
        # utils.print_log('create adressen tabel: Create R-Tree index on geometry column')
        # self.connection.execute(
        #     "CREATE INDEX geom_idx ON adressen USING RTREE (geometry)")
        #
        # utils.print_log('create adressen tabel: Create R-Tree index on lon_lat column')
        # self.connection.execute(
        #     "CREATE INDEX ll_idx ON adressen USING RTREE (lon_lat)")

        # self.connection.commit()

    def adressen_import_meerdere_panden(self):

        # Verblijfsobjecten can be linked to multiple Panden (case for roughly 33k5 of them)
        # In initial ingestion these are encoded as \t (tab) separated pand_id-s in
        # the verblijfsobjecten table.
        # meaning that initial insert into adressen table will not match for these instances
        # (because a combined pand_id in verblijfsobjecten table will not be equal to any
        # id in the panden table).
        # In the end we want to combine the geometries of all Panden involved and take
        # the earliest bouwjaar (year of build), since having bouwjaar as a list is a bit
        # of a pain.

        # So we construct a view that has the unnested combination of verblijfsobjecten and panden.
        # e.g. a verblijfsobject with two panden linked to it will appear twice in this view
        # each with a single pand_id (and bouwjaar and geometry).
        # This can then be folded back into adressen by taking earliest bouwjaar and
        # the combined geometry (provided by spatial st_collect function of DuckDB)

        # unnest multiple pand_id-s
        self.connection.execute("""
            CREATE OR REPLACE TEMP VIEW temp_vo_pand_id AS
            SELECT id, unnest(split(pand_id, e'\t')) AS pand_id
            FROM verblijfsobjecten where pand_id like e'%\t%';
        """)
        # Create another view which combines with geometry
        self.connection.execute("""
            CREATE OR REPLACE TEMP VIEW temp_vo_pand_geometries AS
            SELECT
                v.id,
                list(v.pand_id) as pand_id,
                min(bouwjaar) as bouwjaar,
                st_collect(list(p.geometry)) as geometry,
                max(p.begindatum_geldigheid) as pand_begindatum_geldigheid,
                max(p. einddatum_geldigheid) as pand_einddatum_geldigheid
            FROM temp_vo_pand_id v LEFT JOIN panden p ON v.pand_id = p.id
            GROUP BY ALL ORDER BY pand_id ASC;
        """)

        # Now create a view that combines verblijfsobject with these
        self.connection.execute("""
            CREATE OR REPLACE TEMP VIEW vo_panden AS
            SELECT t.id, v.* EXCLUDE (v.id, v.pand_id), t.* EXCLUDE (t.id)
            FROM temp_vo_pand_geometries t LEFT JOIN verblijfsobjecten v ON t.id=v.id;
        """)

        # Update (insert/replace) the adressen table with this combined verblijfsobject/panden table
        # But only for those present in that combined table, so use right join
        # and filter on nummers.id not NULL
        self.connection.execute("""
            INSERT OR REPLACE INTO adressen (
                nummer_id,
                nummer_begindatum_geldigheid,
                nummer_einddatum_geldigheid,
                pand_id,
                pand_begindatum_geldigheid,
                pand_einddatum_geldigheid,
                verblijfsobject_id,
                gemeente_id,
                woonplaats_id,
                openbare_ruimte_id,
                object_type,
                gebruiksdoel,
                postcode,
                huisnummer,
                huisletter,
                toevoeging,
                oppervlakte,
                rd_x,
                rd_y,
                longitude,
                latitude,
                lon_lat,
                bouwjaar,
                geometry)
            SELECT
                n.id AS nummer_id,
                n.begindatum_geldigheid,
                n.einddatum_geldigheid,
                v.pand_id,
                v.pand_begindatum_geldigheid,
                v.pand_einddatum_geldigheid,
                v.id AS verblijfsobject_id,
                w.gemeente_id,
                o.woonplaats_id,
                o.id,
                'verblijfsobject',
                split(v.gebruiksdoel,e'\t') as gebruiksdoel,
                n.postcode,
                n.huisnummer,
                n.huisletter,
                n.toevoeging,
                v.oppervlakte,
                v.rd_x,
                v.rd_y,
                v.longitude,
                v.latitude,
                v.lon_lat,
                v.bouwjaar,
                v.geometry
            FROM nummers n
            RIGHT JOIN openbare_ruimten o  ON o.id            = n.openbare_ruimte_id
            RIGHT JOIN woonplaatsen w      ON w.woonplaats_id = o.woonplaats_id
            RIGHT JOIN vo_panden v ON v.nummer_id     = n.id
            WHERE n.id IS NOT NULL;
        """)
        # We could have done this slightly simpler with an update statement as below
        # But that turns out to be 1.5s (~ 25%) *slower* than 'insert or replace'.
        # self.connection.execute("""
        #     UPDATE adressen SET
        #         pand_id = p.pand_id,
        #         pand_begindatum_geldigheid = p.pand_begindatum_geldigheid,
        #         pand_einddatum_geldigheid = p.pand_einddatum_geldigheid,
        #         bouwjaar = p.bouwjaar,
        #         geometry = p.geometry,
        #     FROM temp_vo_pand_geometries p
        #     WHERE p.id = adressen.verblijfsobject_id;
        # """)

    def adressen_import_ligplaatsen(self):
        self.connection.execute("""
            UPDATE adressen SET
              rd_x = l.rd_x,
              rd_y = l.rd_y,
              latitude = l.latitude,
              longitude = l.longitude,
              lon_lat = st_point(l.longitude, l.latitude),
              geometry = l.geometry,
              object_type = 'ligplaats'
            FROM ligplaatsen AS l
            WHERE l.nummer_id = adressen.nummer_id;           
        """)

    def adressen_import_standplaatsen(self):
        self.connection.execute("""
            UPDATE adressen SET
              rd_x = s.rd_x,
              rd_y = s.rd_y,
              latitude = s.latitude,
              longitude = s.longitude,
              lon_lat = st_point(s.longitude, s.latitude),
              geometry = s.geometry,
              object_type = 'standplaats'
            FROM standplaatsen AS s
            WHERE s.nummer_id = adressen.nummer_id;
        """)

    def adressen_update_nevenadressen(self):

        # Create unnested view of hoofdadressen and nevenadressen
        self.connection.execute("""
            CREATE OR REPLACE TEMP VIEW nevenadressen AS
            SELECT 
                unnest(split(nevenadressen, e'\t')) as neven_nummer_id,
                nummer_id as hoofd_nummer_id
            FROM verblijfsobjecten 
            WHERE nevenadressen IS NOT NULL;
        """)

        # Update the hoofd_nummer_id for each nevenadres
        self.connection.execute("""
            UPDATE adressen SET
                hoofd_nummer_id = n.hoofd_nummer_id,
            FROM nevenadressen AS n
            WHERE n.neven_nummer_id = adressen.nummer_id;
        """)

        # self.connection.execute("""
        #     DROP TABLE IF EXISTS nevenadressen;
        #
        #     CREATE TEMP TABLE nevenadressen (
        #     neven_nummer_id TEXT PRIMARY KEY,
        #     hoofd_nummer_id TEXT
        # );""")
        #
        # adressen = self.fetchall(
        #     "SELECT nummer_id, nevenadressen FROM verblijfsobjecten WHERE nevenadressen <> ''")
        # parameters = []
        # for adres in adressen:
        #     neven_nummer_ids = adres[1].split('\t')
        #     for neven_id in neven_nummer_ids:
        #         parameters.append([adres[0], neven_id])
        #
        # sql = "INSERT INTO nevenadressen (hoofd_nummer_id, neven_nummer_id) VALUES (?, ?)"
        # if len(parameters) > 0:
        #     self.connection.executemany(sql, parameters)
        #
        # self.connection.execute("""
        #     UPDATE adressen SET
        #         hoofd_nummer_id = n.hoofd_nummer_id,
        #         pand_id = n.pand_id,
        #         verblijfsobject_id = n.verblijfsobject_id,
        #         gebruiksdoel = n.gebruiksdoel,
        #         oppervlakte = n.oppervlakte,
        #         rd_x = n.rd_x,
        #         rd_y = n.rd_y,
        #         latitude = n.latitude,
        #         longitude = n.longitude,
        #         bouwjaar = n.bouwjaar,
        #         geometry = n.geometry
        #     FROM (
        #         SELECT
        #             nevenadressen.hoofd_nummer_id,
        #             nevenadressen.neven_nummer_id,
        #             adressen.pand_id,
        #             adressen.verblijfsobject_id,
        #             adressen.gebruiksdoel,
        #             adressen.oppervlakte,
        #             adressen.rd_x,
        #             adressen.rd_y,
        #             adressen.latitude,
        #             adressen.longitude,
        #             adressen.bouwjaar,
        #             adressen.geometry
        #         FROM nevenadressen
        #         LEFT JOIN adressen ON nevenadressen.hoofd_nummer_id = adressen.nummer_id
        #          ) AS n
        #     WHERE n.neven_nummer_id = adressen.nummer_id;
        # """)

    # woonplaats_id in nummers overrule woonplaats_id van de openbare ruimte.
    def adressen_update_woonplaatsen_from_nummers(self):
        self.connection.execute("""
            UPDATE adressen SET
              woonplaats_id = n.woonplaats_id
            FROM (SELECT id, woonplaats_id FROM nummers WHERE woonplaats_id is not NULL) AS n
            WHERE n.id = adressen.nummer_id;
        """)

    def delete_no_longer_needed_bag_tables(self):
        self.connection.execute("""
          DROP TABLE IF EXISTS nummers; 
          DROP TABLE IF EXISTS panden; 
          DROP TABLE IF EXISTS verblijfsobjecten; 
          DROP TABLE IF EXISTS ligplaatsen; 
          DROP TABLE IF EXISTS standplaatsen; 
        """)

    def adressen_remove_dummy_values(self):
        # The BAG contains dummy values in some fields (bouwjaar, oppervlakte)
        # See: https://geoforum.nl/t/zijn-dummy-waarden-in-de-bag-toegestaan/9091/5

        # Amsterdam heeft een reeks van panden met dummy bouwjaar 1005
        # https://www.amsterdam.nl/stelselpedia/bag-index/catalogus-bag/objectklasse-pand/bouwjaar-pand/
        panden = self.fetchall(f"SELECT pand_id, bouwjaar FROM adressen WHERE bouwjaar=1005;")
        aantal = len(panden)
        utils.print_log(f"fix: test adressen met dummy bouwjaar 1005 in Amsterdam: {aantal: n}")
        if aantal > 0:
            utils.print_log(f"fix: verwijder {aantal:n} ongeldige 1005 bouwjaren")
            self.connection.execute(f"UPDATE adressen SET bouwjaar=NULL WHERE bouwjaar=1005;")

        # The BAG contains some buildings with bouwjaar 9999
        last_valid_build_year = 2040
        panden = self.fetchall(
            f"SELECT pand_id, bouwjaar FROM adressen WHERE bouwjaar > {last_valid_build_year}")
        aantal = len(panden)

        # Show max first 10 items with invalid build year
        panden = panden[slice(10)]

        text_panden = ''
        for pand in panden:
            if text_panden:
                text_panden += ','
            text_panden += pand[0] + ' ' + str(pand[1])
        if text_panden:
            text_panden = f" | panden: {text_panden}"

        utils.print_log(
            f"fix: test adressen met ongeldig bouwjaar > {last_valid_build_year}: {aantal: n}{text_panden}")

        if aantal > 0:
            utils.print_log(f"fix: verwijder {aantal:n} ongeldige bouwjaren (> {last_valid_build_year})")
            self.connection.execute(f"UPDATE adressen SET bouwjaar=NULL WHERE bouwjaar > {last_valid_build_year}")

        # The BAG contains some residences with oppervlakte 999999
        verblijfsobject_ids = self.fetchall(
            "SELECT verblijfsobject_id FROM adressen WHERE oppervlakte = 999999;")
        aantal = len(verblijfsobject_ids)

        text_ids = ''
        for verblijfsobject_id in verblijfsobject_ids:
            if text_ids:
                text_ids += ','
            text_ids += verblijfsobject_id[0]
        if text_ids:
            text_ids = f" | verblijfsobject_ids: {text_ids}"

        utils.print_log(f"fix: test adressen met ongeldige oppervlakte = 999999: {aantal: n}{text_ids}")
        if aantal > 0:
            utils.print_log(f"fix: verwijder {aantal:n} ongeldige oppervlaktes (999999)")
            self.connection.execute("UPDATE adressen SET oppervlakte=NULL WHERE oppervlakte = 999999;")

        # The BAG contains some residences with oppervlakte 1 (In Amsterdam this is a valid dummy)
        # https://www.amsterdam.nl/stelselpedia/bag-index/catalogus-bag/objectklasse-vbo/gebruiksoppervlakte/
        verblijfsobject_ids = self.fetchall(
            "SELECT verblijfsobject_id FROM adressen WHERE oppervlakte = 1;")
        aantal = len(verblijfsobject_ids)
        utils.print_log(f"fix: test adressen met ongeldige oppervlakte = 1 (dummy value in Amsterdam): {aantal: n}")
        if aantal > 0:
            utils.print_log(f"fix: verwijder {aantal:n} met ongeldige oppervlakte = 1")
            self.connection.execute("UPDATE adressen SET oppervlakte=NULL WHERE oppervlakte = 1;")

        # The BAG contains some addresses without valid public space
        address_count = self.fetchone(
            "SELECT COUNT(*) FROM adressen WHERE openbare_ruimte_id IS NULL "
            " OR openbare_ruimte_id NOT IN (SELECT id FROM openbare_ruimten);")
        utils.print_log("fix: test adressen zonder openbare ruimte: " + str(address_count))

        # Delete them if not too many
        if (address_count > 0) and (address_count < config.delete_addresses_without_public_spaces_if_less_than):
            utils.print_log(f"fix: verwijder {address_count:n} adressen zonder openbare ruimte")
            self.connection.execute("DELETE FROM adressen WHERE openbare_ruimte_id IS NULL "
                                    "OR openbare_ruimte_id NOT IN (SELECT id FROM openbare_ruimten)")

    def table_exists(self, table_name):
        # Check if database contains adressen tabel
        count = self.fetchone(
            f"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '{table_name}';")
        return count == 1

    def test_bag_adressen(self) -> bool:
        """
            Tests the BAG (Basisregistratie Adressen en Gebouwen) data integrity.

            This method checks if there are any errors in the database related to municipalities
            without associated addresses and other possible issues.

            Returns:
                bool: True if no errors were found (total_error_count == 0),
                      False otherwise.
            """
        total_error_count = 0

        if not self.table_exists('adressen'):
            utils.print_log("DuckDB database bevat geen adressen tabel. Importeer BAG eerst.", True)
            quit()

        utils.print_log(f"start: tests op BAG DuckDB database: '{config.file_db_duckdb}'")

        sql = "SELECT nummer_begindatum_geldigheid FROM adressen ORDER BY nummer_begindatum_geldigheid DESC LIMIT 1"
        datum = self.fetchone(sql)
        utils.print_log(f"info: laatste nummer_begindatum_geldigheid: {datum}")

        sql = "SELECT pand_begindatum_geldigheid FROM adressen ORDER BY pand_begindatum_geldigheid DESC LIMIT 1"
        datum = self.fetchone(sql)
        utils.print_log(f"info: laatste pand_begindatum_geldigheid: {datum}")

        # Soms zitten er nog oude gemeenten die niet meer bestaan in de gemeenten.csv filee
        count = self.fetchone("""
            SELECT COUNT(*) FROM gemeenten
            WHERE id NOT IN (SELECT DISTINCT gemeente_id FROM adressen);
            """)
        total_error_count += count
        utils.print_log("test: gemeenten zonder adressen: " + str(count), count > 0)

        if count > 0:
            gemeenten = self.fetchall("""
                SELECT id, naam FROM gemeenten 
                WHERE id NOT IN (SELECT DISTINCT gemeente_id FROM adressen);
                """)

            gemeenten_formatted = ', '.join(f"{gemeente[0]} {gemeente[1]}" for gemeente in gemeenten)

            utils.print_log("test: gemeenten zonder adressen: " + gemeenten_formatted, count > 0)

        count = self.fetchone("""
            SELECT COUNT(*) FROM woonplaatsen 
            WHERE gemeente_id IS NULL OR gemeente_id NOT IN (SELECT id FROM gemeenten);
            """)
        total_error_count += count
        utils.print_log("test: woonplaatsen zonder gemeente: " + str(count), count > 0)

        count = self.fetchone("""
            SELECT COUNT(*) FROM adressen 
            WHERE openbare_ruimte_id IS NULL
                OR openbare_ruimte_id NOT IN (SELECT id FROM openbare_ruimten)
            """)
        total_error_count += count
        utils.print_log("test: adressen zonder openbare ruimte: " + str(count), count > 0)

        count = self.fetchone("SELECT COUNT(*) FROM adressen WHERE woonplaats_id IS NULL;")
        total_error_count += count
        utils.print_log("test: adressen zonder woonplaats: " + str(count), count > 0)

        count = self.fetchone("SELECT COUNT(*) FROM adressen WHERE gemeente_id IS NULL;")
        total_error_count += count
        utils.print_log("test: adressen zonder gemeente: " + str(count), count > 0)

        # Het is makkelijk om per ongeluk een gemeenten.csv te genereren die niet in UTF-8 is. Testen dus.
        naam = self.fetchone("SELECT naam FROM gemeenten WHERE id=1900")
        is_error = naam != 'Súdwest-Fryslân'
        if is_error: total_error_count += 1
        utils.print_log("test: gemeentenamen moeten in UTF-8 zijn: " + naam, is_error)

        count = self.fetchone(
            "SELECT COUNT(*) FROM adressen WHERE adressen.latitude IS NULL AND pand_id IS NOT NULL;")
        total_error_count += count
        utils.print_log("test: panden zonder locatie: " + str(count), count > 0)

        count = self.fetchone("SELECT COUNT(*) FROM adressen "
                              "WHERE adressen.latitude IS NULL AND object_type='ligplaats';")
        total_error_count += count
        utils.print_log("test: ligplaatsen zonder locatie: " + str(count), count > 0)

        count = self.fetchone("SELECT COUNT(*) FROM adressen "
                              "WHERE adressen.latitude IS NULL AND object_type='standplaats';")
        total_error_count += count
        utils.print_log("test: standplaatsen zonder locatie: " + str(count), count > 0)

        # Sommige nummers hebben een andere woonplaats dan de openbare ruimte waar ze aan liggen.
        woonplaats_id = self.fetchone(
            "SELECT woonplaats_id FROM adressen WHERE postcode='1181BN' AND huisnummer=1;")
        is_error = woonplaats_id != 1050
        if is_error: total_error_count += 1
        utils.print_log("test: nummeraanduiding WoonplaatsRef tag. 1181BN-1 ligt in Amstelveen (1050). "
                        f"Niet Amsterdam (3594): {woonplaats_id:n}", is_error)

        count = self.fetchone("SELECT COUNT(*) FROM adressen;")
        is_error = count < 9000000
        if is_error: total_error_count += 1
        utils.print_log(f"info: adressen: {count:n}", is_error)

        count = self.fetchone("SELECT COUNT(*) FROM adressen WHERE pand_id IS NOT NULL;")
        is_error = count < 9000000
        if is_error: total_error_count += 1
        utils.print_log(f"info: panden: {count:n}", is_error)

        count = self.fetchone("SELECT COUNT(*) FROM adressen WHERE object_type='ligplaats';")
        is_error = count < 10000
        if is_error: total_error_count += 1
        utils.print_log(f"info: ligplaatsen: {count:n}", is_error)

        count = self.fetchone("SELECT COUNT(*) FROM adressen WHERE object_type='standplaats';")
        is_error = count < 20000
        if is_error: total_error_count += 1
        utils.print_log(f"info: standplaatsen: {count:n}", is_error)

        count = self.fetchone("SELECT COUNT(*) FROM openbare_ruimten;")
        is_error = count < 250000
        if is_error: total_error_count += 1
        utils.print_log(f"info: openbare ruimten: {count:n}", count < is_error)

        count = self.fetchone("SELECT COUNT(*) FROM woonplaatsen;")
        is_error = count < 2000
        if is_error: total_error_count += 1
        utils.print_log(f"info: woonplaatsen: {count:n}", is_error)

        count = self.fetchone("SELECT COUNT(*) FROM gemeenten;")
        is_error = count < 300
        if is_error: total_error_count += 1
        utils.print_log(f"info: gemeenten: {count}", is_error)

        count = self.fetchone("SELECT COUNT(*) FROM provincies;")
        is_error = count != 12
        if is_error: total_error_count += 1
        utils.print_log(f"info: provincies: {count}", is_error)

        utils.print_log(f"test: total errors: {total_error_count}", total_error_count > 0)

        return total_error_count == 0
