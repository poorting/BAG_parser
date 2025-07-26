#! /usr/bin/env python3
from argparse import ArgumentParser

from exporter import Exporter

parser = ArgumentParser(description='Export addresses or postcodes in DuckDB database to a Parquet (default), DuckDB, TSV or JSON file')

helpText = ("Export all data including year of construction, latitude, longitude, floor area and intended use of "
            "buildings")
parser.add_argument('-a', '--all', action='store_true', help=helpText)

helpText = ("Export all data as above, but including geometry as well (if generated)")
parser.add_argument('-ag', '--geometry', action='store_true', help=helpText)

helpText = "Export statistics of 4 character postal code groups (e.g. 1000)"
parser.add_argument('-p4', '--postcode4', action='store_true', help=helpText)

helpText = "Export statistics of 5 character postal code groups (e.g. 1000A)"
parser.add_argument('-p5', '--postcode5', action='store_true', help=helpText)

helpText = "Export statistics of 6 character postal code groups (e.g. 1000AA)"
parser.add_argument('-p6', '--postcode6', action='store_true', help=helpText)

helpText = "Export as TSV (Tab Separated Values) rather than Parquet"
parser.add_argument('--tsv', action='store_true', help=helpText)

helpText = "Export as JSON rather than Parquet"
parser.add_argument('--json', action='store_true', help=helpText)

helpText = "Export as DuckDB rather than Parquet"
parser.add_argument('--duckdb', action='store_true', help=helpText)

args = parser.parse_args()

exporter = Exporter()

ext = 'parquet'
export_options = "(FORMAT parquet)"
if args.tsv:
    ext = 'tsv'
    export_options = "(HEADER, DELIMITER '\t')"
elif args.json:
    ext = 'json'
    export_options = "(ARRAY)"
elif args.duckdb:
    ext = 'duckdb'
    if args.all or args.geometry:
        export_options = 'adressen'
    else:
        export_options = 'postcode'

if args.all:
    exporter.export(f'output/adressen_all_data.{ext}', export_options, False)
elif args.geometry:
    exporter.export(f'output/adressen_all_data_geometry.{ext}', export_options, True)
elif args.postcode4:
    exporter.export_postcode4_stats(f'output/adressen_p4_stats.{ext}', export_options)
elif args.postcode5:
    exporter.export_postcode5_stats(f'output/adressen_p5_stats.{ext}', export_options)
elif args.postcode6:
    exporter.export_postcode6_stats(f'output/adressen_p6_stats.{ext}', export_options)
else:
    exporter.export_postcode(f'output/adressen_postcodes.{ext}', export_options)
