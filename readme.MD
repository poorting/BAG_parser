[![GitHub license](https://img.shields.io/github/license/digitaldutch/BAG_parser)](https://github.com/digitaldutch/BAG_parser/blob/master/LICENSE)
[![GitHub issues](https://img.shields.io/github/issues/digitaldutch/BAG_parser)](https://github.com/digitaldutch/BAG_parser/issues)

# Digital Dutch BAG parser

Original is by [digitaldutch](https://github.com/digitaldutch/BAG_parser) and uses SQLite as a database. I simply changed it to use DuckDB instead for the following reasons:
* DuckDB is way faster in analysis (but slower in ingesting)
* Very nice integration with Python (e.g. returning results as a dataframe)
* Native ability to export to Parquet, TSV/CSV and JSON (see export section)
  * Really, you should use Parquet over TSV/CSV...
* Because why not? :-)

Further improvements are the proper processing of geometry for Panden (later linked to adressen) which weren't properly processed by the original, as well as storing these geometries in a proper format (DuckDB 'GEOMETRY' data type).
Note that by default parsing of geometries is disabled in config.py. Set 'parse_geometries' to True to enable it. 
Regardless of this setting, locations are now stored as a single POINT(longitude, latitude) as well (lon_lat field), next to the separate longitude and latitude fields.

DuckDB has spatial support, which can be enabled with
```
INSTALL Spatial;
LOAD Spatial;
INSTALL JSON;
LOAD JSON;
```
Json extension is needed if you want to use GeoJSON (e.g. for import/export).
When exporting geometries (see below) they are exported as BLOB for (Geo)Parqet, as GeoJSON for JSON and WKT (Well-Known Text) for TSV.

You can use DuckDB's [spatial functions](https://duckdb.org/docs/stable/core_extensions/spatial/functions.html) to work with geometry or lon_lat fields, e.g. to find all adresses within a certain (spatial) area.

## TL;DR
Converts in (quite) a few minutes the big, complex and hard to read XML Dutch addresses database (BAG or Basisregistratie Adressen
en Gebouwen) into a user-friendly, file-based, blazingly fast ~~SQLite~~ **DuckDB** database by running a single Python script. 
No need to install any dependencies or a database server.

Another oneliner script (see below) will convert this ~~SQLite~~ **DuckDB** database to ~~CSV~~ Parquet_(or CSV or JSON)_ in ~~15 seconds~~ seconds. 

## Download the parsed BAG
If you don't want to run the script yourself, download the latest BAG in ~~SQLite or CSV~~ parquet format from
the [releases](https://github.com/poorting/BAG_parser/releases) section.

## About the BAG
The Dutch public addresses and buildings database (BAG or Basisregistratie Adressen en Gebouwen) is freely downloadable
from the [Dutch cadastre](https://www.kadaster.nl/-/gratis-download-bag-extract) agency named Kadaster. Hooray 🙂. 

The bad news is: The original BAG comes in a complex and hard to read XML format using thousands of zipped XML files, 
which will quickly reduce your initial enthusiasm. 
It also does not include municipalities or provinces and provides coordinates using a system that non-experts won't 
expect named 
 [Rijksdriehoekscoördinaten](https://nl.wikipedia.org/wiki/Rijksdriehoeksco%C3%B6rdinaten)😲. 

## What this parser does
This Python utility parses the BAG database and converts it into a clean, easy to read & use DuckDB database.
Municipalities (gemeenten) and provinces (provincies) are added. Rijksdriehoekscoördinaten coordinates are converted 
to standard WGS84 latitude and longitude coordinates. Invalid (dummy) bouwjaar and oppervlakte fields are removed. 
Construction year, floor area and intended use of buildings are also provided. 
Several tables (nummers, verblijfsobjecten, panden, ligplaatsen and standplaatsen) are merged into a general 'adressen'
table. The DuckDB database can be used directly as a source to generate a *.csv, *.parquet or *.json file or to update your own addresses 
databases. There are a couple of options available in the [config.py](config.py).

## Requirements
* Python 3.13. Older Python versions may work but are not tested and certainly slower.

## Usage
* Download or use git (recommended as updates are easier) to download the BAG parser.   
  Git command for initial checkout:  
  `git clone https://github.com/poorting/BAG_parser`   
  Update to the latest version:  
  `git pull https://github.com/poorting/BAG_parser`
* Create a Python Virtual Environment with the required modules:
``` 
python3 -m venv venv
source venv/bin/activate
pip install wheel
pip install -r requirements.txt
```
_Note: You might get an error message at the first step (most likely to happen if you're on Debian). In which case the venv python package is not installed yet. In that case install it first by doing:_

```
sudo apt install python3-venv 
```
_Then follow the steps for creating the virtual environment again._

* Download the BAG (3 GB) from [kadaster.nl](https://www.kadaster.nl/-/gratis-download-bag-extract)
  or directly from [pdok.nl](https://service.pdok.nl/kadaster/adressen/atom/v1_0/downloads/lvbag-extract-nl.zip)
  and save the file as `bag.zip` in the `input` folder.
* The [gemeenten-alfabetisch-2025.xlsx](input/gemeenten-alfabetisch-2025.xlsx) file is already included in the `input` folder, but you can [download the latest version from the CBS website](https://www.cbs.nl/nl-nl/onze-diensten/methoden/classificaties/overig/gemeentelijke-indelingen-per-jaar). Save it in the input folder and if needed change config.py to reference the correct file.
* Set your options in [config.py](config.py)
* Run `./import_bag.py`
* Drink a cup of coffee for a few minutes ☕😎 while watching the progress bar.
* Open the DuckDB database with your favorite tool. I like the direct [DuckDB CLI](https://duckdb.org/docs/installation/), although a tool like [DBeaver](https://dbeaver.io/) works as well.
Here's an example query on DuckDB database to get information about postcode 2514GL, huisnummer 78 (Paleis Noordeinde):
```SQL
SELECT
  a.postcode,
  a.huisnummer,
  a.huisletter || a.toevoeging AS toevoeging,
  o.naam                       AS straat,
  g.naam                       AS gemeente,
  w.naam                       AS woonplaats,
  p.naam                       AS provincie,
  a.bouwjaar,
  a.latitude,
  a.longitude,
  a.rd_x,
  a.rd_y,
  a.oppervlakte                AS vloeroppervlakte,
  a.gebruiksdoel,
  a.hoofd_nummer_id
FROM adressen a
  LEFT JOIN openbare_ruimten o ON a.openbare_ruimte_id = o.id
  LEFT JOIN gemeenten g        ON a.gemeente_id        = g.id
  LEFT JOIN woonplaatsen w     ON a.woonplaats_id      = w.woonplaats_id
  LEFT JOIN provincies p       ON g.provincie_id       = p.id
WHERE postcode = '2514GL'
  AND huisnummer = 68;
```
This should result in something like:
```commandline
┌──────────┬────────────┬────────────┬────────────┬───────────────┬───────────────┬──────────────┬──────────┬──────────┬───────────┬──────────┬───────────┬──────────────────┬─────────────────────────┬─────────────────┐
│ postcode │ huisnummer │ toevoeging │   straat   │   gemeente    │  woonplaats   │  provincie   │ bouwjaar │ latitude │ longitude │   rd_x   │   rd_y    │ vloeroppervlakte │      gebruiksdoel       │ hoofd_nummer_id │
│ varchar  │   int32    │  varchar   │  varchar   │    varchar    │    varchar    │   varchar    │  int32   │  float   │   float   │  float   │   float   │      float       │         varchar         │     varchar     │
├──────────┼────────────┼────────────┼────────────┼───────────────┼───────────────┼──────────────┼──────────┼──────────┼───────────┼──────────┼───────────┼──────────────────┼─────────────────────────┼─────────────────┤
│ 2514GL   │     68     │ NULL       │ Noordeinde │ 's-Gravenhage │ 's-Gravenhage │ Zuid-Holland │   1814   │ 52.08101 │ 4.3066735 │ 80933.69 │ 455299.28 │     11923.0      │ overige gebruiksfunctie │ NULL            │
└──────────┴────────────┴────────────┴────────────┴───────────────┴───────────────┴──────────────┴──────────┴──────────┴───────────┴──────────┴───────────┴──────────────────┴─────────────────────────┴─────────────────┘
Run Time (s): real 0.067 user 0.620957 sys 0.034688
```

* When done parsing, use [export.py](export.py) to create a *.parquet, *.tsv or *.json file. This file has several command line options (see below). 
These conversion functions are easy to customize. 
Parquet is a very common (binary, compressed, self-describing) format these days for importing data into other tools.
## Python commands ##

### [import_bag.py](import_bag.py)
Parses the original BAG file and transforms it into a DuckDB database. Takes about 12 minutes to complete
on a MacBook Pro (M1 Pro), roughly 20 minutes on an aging AMD 5 2600; or a few minutes more if you switch on the `parse_geometries` option in the [config.py](config.py).

### [export.py](export.py)
Exports the addresses in DuckDB database to a *.parquet (default), *.csv or *.json file. By default, only the addresses and
postcode data are exported (~1 second). Use the command options below for more output formats.  

```
./export.py -h 
usage: export.py [-h] [-a] [-ag] [-p4] [-p5] [-p6] [--tsv] [--json]

Export addresses in DuckDB database to a Parquet, TSV or JSON file

options:
  -h, --help        show this help message and exit
  -a, --all         Export all data including year of construction, latitude, longitude, floor area and intended use
                    of buildings
  -ag, --geometry   Export all data as above, but including geometry as well (if generated)
  -p4, --postcode4  Export statistics of 4 character postal code groups (e.g. 1000)
  -p5, --postcode5  Export statistics of 5 character postal code groups (e.g. 1000A)
  -p6, --postcode6  Export statistics of 6 character postal code groups (e.g. 1000AA)
  --tsv             Export to TSV (Tab Separated Values) rather than Parquet
  --json            Export to JSON rather than Parquet
```

### [test_sqlite_db.py](test_sqlite_db.py)
Checks the DuckDB database for info and errors. `import_bag.py` also performs these tests after parsing.

### [utils_sqlite_shrink.py](utils_sqlite_shrink.py)
Reduces the DuckDB database size by removing BAG tables (nummers, verblijfsobjecten, panden, ligplaatsen and standplaatsen) 
that are no longer needed due to the new 'adressen' table.
The parser also does this as a final step if `delete_no_longer_needed_bag_tables` is set to `True` in [config.py](config.py).
Note that this currently does **not** reduce the file size of the database.

## Adressen table
An adres is a nevenadres if the `hoofd_nummer_id` field is set. It points to the `nummer_id` of the hoofdadres. 

## Limitations and notes
* The WGS84 coordinates are calculated using [approximation equations by F.H. Schreutelkamp and G.L. Strang van Hees](docs/Benaderingsformules_RD_WGS.pdf). This conversion has an error of a few decimeters. Don't use the 
WGS84 coordinates if you need higher accuracy. 
* verblijfsobjecten table:  
  Some gebruiksdoel, pand_id and nevenadressen fields contain multiple,
  comma-separated, values. Be careful if you do queries with joins on those fields.
* Adressen table:  
  * Some gebruiksdoel and pand_id fields contain multiple, comma-separated, values.
  * The bouwjaar and geometry field only contain the data of one pand, even if an address has multiple panden. 
* There are probably several more things missing that I don't know about. Feel free to file a [GitHub issue](https://github.com/digitaldutch/BAG_parser/issues).

## Documents
* [Praktijkhandleiding BAG](https://imbag.github.io/praktijkhandleiding/) 
* [Catalogus Basisregistratie Adressen en Gebouwen](https://www.geobasisregistraties.nl/documenten/publicatie/2018/03/12/catalogus-2018)

## Tools

#### Official BAG viewer
The Kadaster has an online [BAG viewer](https://bagviewer.kadaster.nl/) where you can search any address or other info in the official database.

#### nlextract
This tool does not parse all data. If you need more data or professional support, buy it from [nlextract](https://nlextract.nl/),
who have a more complex, but also complete [parser](https://github.com/nlextract/NLExtract).

#### bagconv
Bert hubert has [written a parser](https://berthub.eu/articles/posts/dutch-postcode-and-building-database/) in C++,
[bagconv](https://github.com/berthubert/bagconv), which is quite similar to this one.

## License
This software is made available under the [MIT license](LICENSE).
