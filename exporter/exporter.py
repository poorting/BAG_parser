# Export DuckDB BAG to csv or other format

import utils
from database_duckdb import DatabaseDuckdb

class Exporter:

    def __init__(self):
        self.database = DatabaseDuckdb()
        self.total_adressen = 0

    def __export(self, output_filename, export_options, sql):

        utils.print_log(f"start: export adressen naar bestand '{output_filename}'")
        self.database.connection.execute("""
            INSTALL Spatial;
            LOAD Spatial;
            INSTALL JSON;
            LOAD JSON;
        """)
        if not self.database.table_exists('adressen'):
            utils.print_log("DuckDB database bevat geen adressen tabel. Importeer BAG eerst.", True)
            quit()

        if not output_filename.endswith('.duckdb'):
            sqlcmd = f"COPY ({sql}) TO '{output_filename}' {export_options};"
            self.database.connection.execute(sqlcmd)
        else:
            self.database.connection.execute(f"ATTACH '{output_filename}' AS export;")
            sqlcmd = f"CREATE OR REPLACE TABLE export.{export_options} AS {sql}"
            self.database.connection.execute(sqlcmd)
            self.database.connection.execute(f"DETACH export;")

    def _lon_lat_export(self, output_filename, export_geometry=False):
        exp_geom = ""
        exp_lon_lat = ""
        if output_filename.endswith('.parquet'):
            exp_lon_lat = "a.lon_lat AS lon_lat,"
            exp_geom = "a.geometry AS geometry" if export_geometry else ""
        elif output_filename.endswith('.json'):
            exp_lon_lat = "st_asgeojson(a.lon_lat) AS lon_lat,"
            exp_geom = "st_asgeojson(a.geometry) as geometry " if export_geometry else ""
        elif output_filename.endswith('.tsv'):
            exp_lon_lat = "st_astext(a.lon_lat) AS lon_lat,"
            exp_geom = "st_astext(a.geometry) as geometry " if export_geometry else ""
        elif output_filename.endswith('.duckdb'):
            exp_lon_lat = "a.lon_lat AS lon_lat,"
            exp_geom = "a.geometry as geometry " if export_geometry else ""

        return exp_geom, exp_lon_lat


    def export(self, output_filename, export_options, export_geometry=False):
        # exp_geom = ""
        # exp_lon_lat = ""
        # if output_filename.endswith('.parquet'):
        #     exp_lon_lat = "a.lon_lat AS lon_lat,"
        #     exp_geom = "a.geometry AS geometry" if export_geometry else ""
        # elif output_filename.endswith('.json'):
        #     exp_lon_lat = "st_asgeojson(a.lon_lat) AS lon_lat,"
        #     exp_geom = "st_asgeojson(a.geometry) as geometry " if export_geometry else ""
        # elif output_filename.endswith('.tsv'):
        #     exp_lon_lat = "st_astext(a.lon_lat) AS lon_lat,"
        #     exp_geom = "st_astext(a.geometry) as geometry " if export_geometry else ""
        # elif output_filename.endswith('.duckdb'):
        #     exp_lon_lat = "a.lon_lat AS lon_lat,"
        #     exp_geom = "a.geometry as geometry " if export_geometry else ""
        exp_geom, exp_lon_lat = self._lon_lat_export(output_filename, export_geometry)

        sql = f"""
                SELECT
                  o.naam                       AS straat,
                  a.huisnummer,
                  concat(a.huisletter,a.toevoeging) AS toevoeging,
                  a.postcode,
                  g.naam                       AS gemeente,
                  g.gm_code                    AS gm_code,
                  w.naam                       AS woonplaats,
                  p.naam                       AS provincie,
                  p.pv_code                    AS pv_code,
                  a.bouwjaar,
                  a.rd_x,
                  a.rd_y,
                  a.latitude,
                  a.longitude,
                  {exp_lon_lat}
                  a.oppervlakte                AS vloeroppervlakte,
                  a.gebruiksdoel,
                  a.hoofd_nummer_id,
                  {exp_geom}
                FROM adressen a
                  LEFT JOIN openbare_ruimten o ON a.openbare_ruimte_id = o.id
                  LEFT JOIN gemeenten g        ON a.gemeente_id        = g.id
                  LEFT JOIN woonplaatsen w     ON a.woonplaats_id      = w.woonplaats_id
                  LEFT JOIN provincies p       ON g.provincie_id       = p.id
        """

        self.__export(output_filename, export_options, sql)

    def export_postcode(self, output_filename, export_options, is_parquet=False):
        exp_geom, exp_lon_lat = self._lon_lat_export(output_filename)
        sql = f"""
            SELECT
              o.naam                       AS straat,
              a.huisnummer,
              concat(a.huisletter,a.toevoeging) AS toevoeging,
              a.postcode,
              a.latitude,
              a.longitude,
              {exp_lon_lat}
              w.naam                       AS woonplaats
            FROM adressen a
              LEFT JOIN openbare_ruimten o ON a.openbare_ruimte_id = o.id
              LEFT JOIN woonplaatsen w     ON a.woonplaats_id      = w.woonplaats_id
        """

        self.__export(output_filename, export_options, sql)

    def export_postcode4_stats(self, output_filename, export_options):
        exp_geom, exp_lon_lat = self._lon_lat_export(output_filename)
        exp_lon_lat = exp_lon_lat.replace("a.lon_lat", "ST_Centroid(ST_Collect(list(a.lon_lat)))")
        sql = f"""
          SELECT
            SUBSTR(a.postcode, 0, 5) AS pc4,
            AVG(a.latitude)          AS center_lat,
            AVG(a.longitude)         AS center_lon,
            {exp_lon_lat}
            COUNT(1)                 AS aantal_adressen,
            FIRST(w.naam)            AS woonplaats
          FROM adressen a
            LEFT JOIN woonplaatsen w ON a.woonplaats_id = w.woonplaats_id
          WHERE a.postcode <> ''
          GROUP BY pc4
        """
        self.__export(output_filename, export_options, sql)

    def export_postcode5_stats(self, output_filename, export_options):
        exp_geom, exp_lon_lat = self._lon_lat_export(output_filename)
        exp_lon_lat = exp_lon_lat.replace("a.lon_lat", "ST_Centroid(ST_Collect(list(a.lon_lat)))")
        sql = f"""
          SELECT
            SUBSTR(a.postcode, 0, 6) AS pc5,
            AVG(a.latitude)          AS center_lat,
            AVG(a.longitude)         AS center_lon,
            {exp_lon_lat}
            COUNT(1)                 AS aantal_adressen,
            FIRST(w.naam)            AS woonplaats
          FROM adressen a
            LEFT JOIN woonplaatsen w ON a.woonplaats_id = w.woonplaats_id
          WHERE a.postcode <> ''
          GROUP BY pc5
        """

        self.__export(output_filename, export_options, sql)

    def export_postcode6_stats(self, output_filename, export_options):
        exp_geom, exp_lon_lat = self._lon_lat_export(output_filename)
        exp_lon_lat = exp_lon_lat.replace("a.lon_lat", "ST_Centroid(ST_Collect(list(a.lon_lat)))")
        sql = f"""
          SELECT
            a.postcode       AS pc6,
            AVG(a.latitude)  AS center_lat,
            AVG(a.longitude) AS center_lon,
            {exp_lon_lat}
            COUNT(1)         AS aantal_adressen,
            FIRST(w.naam)    AS woonplaats
          FROM adressen a
            LEFT JOIN woonplaatsen w ON a.woonplaats_id = w.woonplaats_id
          WHERE a.postcode <> ''
          GROUP BY pc6
        """

        self.__export(output_filename, export_options, sql)
