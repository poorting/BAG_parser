# BAG XML parser
import math
import os
import time
from concurrent.futures import ProcessPoolExecutor, wait
from lxml import etree
from lxml.etree import ElementTree
import config

import utils
from bag import rijksdriehoek

FIND_FIELD = 0
FIND_FIELD_MULTI = 1
FIND_NESTED_FIELD = 2
FIND_NESTED_FIELD_MULTI = 3

def prettyprint(element, prepend=''):
    # xml = etree.tostring(element, pretty_print=True)
    # print(xml.decode(), end='')
    name = element.tag.split("}")[1][0:]
    value = f"({element.text})" if element.text and len(element.text.strip()) > 1 else ""
    attrs = ""
    for k, v in element.items():
        attrs += f"{k}={v} "
    print(f"{prepend}{name} {value} {attrs}")
    if "Polygon" != name:
        for child in element:
            prettyprint(child, prepend + '\t')


def parse_xml_file(file_xml, tag_name, data_init, object_tag_name, db_fields):
    today_string = utils.bag_date_today()

    def find_field(bag_element, field_name):
        # print(f"find_field {bag_element}, {nested_list}")
        field = bag_element.findall('.//{*}' + field_name)
        if field:
            return field[0].text
        return None

    def find_field_multi(bag_element, field_name):
        # print(f"find_field {bag_element}, {nested_list}")
        fields = bag_element.findall('.//{*}' + field_name)
        if fields:
            doelen = [field.text for field in fields]
            return "\t".join(doelen)
        return None

    def find_nested_field(bag_element, nested_list):
        # print(f"find_nested {bag_element}, {nested_list}")
        nested_list1 = nested_list.copy()
        nested_field = nested_list1.pop(0)
        field = bag_element.findall('.//{*}' + nested_field)
        if field:
            # print(f"\tfound: {field}")
            if nested_list1:
                return find_nested_field(field[0], nested_list1)
            else:
                # print(f"\tvalue: {field[0].text}")
                return field[0].text

        return None

    def find_nested_field_multi(bag_element, nested_list):
        # print(f"find_nested {bag_element}, {nested_list}")
        nested_list1 = nested_list.copy()
        nested_field = nested_list1.pop(0)
        fields = bag_element.findall('.//{*}' + nested_field)
        if fields:
            result_list = None
            # if len(fields) > 1:
            #     print(f"\tfound: {fields} {len(fields)} times")
            if nested_list1:
                for field in fields:
                    # return find_nested(field[0], nested_list1)
                    res = find_nested_field_multi(field, nested_list1)
                    if res:
                        if not result_list:
                            result_list = res
                        else:
                            result_list.extend(res)
                return result_list
            else:
                # print(f"\tvalue: {field[0].text}")
                # return [field.text for field in fields]
                doelen = [field.text for field in fields]
                return "\t".join(doelen)

        return None

    def bag_begindatum_valid(data):
        datum = data.get('begindatum_geldigheid')
        if datum:
            datum = datum[0:10]
            # string date compare is quicker than converting to date types
            return datum <= today_string
        else:
            return False

    def bag_einddatum_valid(data):
        datum = data.get('einddatum_geldigheid')
        if datum:
            datum = datum[0:10]
            # string date compare is quicker than converting to date types
            return datum >= today_string
        else:
            return True
            # No einddatum means valid

    def data_active(data):
        status_ok = (not status_active) or (data['status'] == status_active)
        return status_ok and bag_begindatum_valid(data) and bag_einddatum_valid(data)

    data = data_init.copy()
    coordinates_field = None
    has_geometry = False
    # list_field = None
    # 2 or 3 coordinates for geometry?
    # (Panden use 3, ligplaats & standplaats use 2)
    geometry_points = 2
    status_active = None
    # Cache data in memory to perform saving to DuckDB in a single loop in the calling main process.
    # DuckDB is not good at writing from multiple processes.
    db_data = []
    xml_count = 0

    match tag_name:
        case 'Woonplaats':
            status_active = 'Woonplaats aangewezen'
            has_geometry = True
        case 'GemeenteWoonplaatsRelatie':
            None
        case 'OpenbareRuimte':
            status_active = 'Naamgeving uitgegeven'
        case 'Nummeraanduiding':
            status_active = 'Naamgeving uitgegeven'
        case 'Pand':
            has_geometry = True
            geometry_points = 3
        case 'Verblijfsobject':
            coordinates_field = 'pos'
            # list_field = 'gebruiksdoel'
        case 'Ligplaats':
            coordinates_field = 'geometry'
            has_geometry = True
        case 'Standplaats':
            coordinates_field = 'geometry'
            has_geometry = True
        case _:
            raise Exception(f'No save function found for tag_name "{tag_name}"')

    root = etree.parse(file_xml).getroot()
    bag_objects = root.findall(".//{*}"+object_tag_name)
    xml_count += len(bag_objects)

    for bag_object in bag_objects:
        data = data_init.copy()
        for db_field, (xml_field, find_function) in db_fields.items():
            # data[db_field] = find_nested(bag_object, xml_field)
            # results = find_function(bag_object, xml_field)
            # print(f"{xml_field} : {results} - {len(results) if results else 'None'}")
            # data[db_field] = find_functions[find_function](bag_object, xml_field)
            if find_function == FIND_FIELD:
                    data[db_field] = find_field(bag_object, xml_field)
            elif find_function == FIND_NESTED_FIELD:
                    data[db_field] = find_nested_field(bag_object, xml_field)
            elif find_function == FIND_FIELD_MULTI:
                    data[db_field] = find_field_multi(bag_object, xml_field)
            elif find_function == FIND_NESTED_FIELD_MULTI:
                data[db_field] = find_nested_field_multi(bag_object, xml_field)
            else:
                utils.print_log(f"Unknown find_field function ({find_function}), setting field {db_field} to None/NULL", error=True)
                data[db_field] = None

        db_data.append(data)

    if config.active_only:
        db_data = list(filter(lambda d: data_active(d), db_data))

    if coordinates_field is not None:
        db_data = add_coordinates(db_data, coordinates_field)

    if has_geometry:
        if config.parse_geometries:
            db_data = geometry_to_wgs84(db_data, geometry_points)
        else:
            db_data = geometry_to_empty(db_data)
    return {'count':xml_count, 'data':db_data}


def geometry_to_wgs84(rows, geometry_points=2):
    for i, row in enumerate(rows):
        row['geometry'] = utils.bag_geometry_to_wgs_geojson(row['geometry'], geometry_points)
    return rows


def geometry_to_empty(rows):
    for i, row in enumerate(rows):
        row['geometry'] = ''

    return rows


def get_pos_from_geometry(data):
    # geometry data looks like this: [111.111 222.222 333.333 444.444]
    # Return string with first two numbers: 111.111 222.222
    first_space = data.find(' ')
    second_space = data.find(' ', first_space + 1)

    return data[1:second_space]


def add_coordinates(rows, field_name):
    for i, row in enumerate(rows):
        if row[field_name]:
            if field_name == 'geometry':
                pos = get_pos_from_geometry(row[field_name])
            else:
                pos = row[field_name]

            [row["rd_x"], row["rd_y"]] = utils.bag_pos_to_rd_coordinates(pos)
            [row["latitude"], row["longitude"]] = rijksdriehoek.rijksdriehoek_to_wgs84(row["rd_x"], row["rd_y"])

    return rows

class BagParser:
    gui_time = None
    folder_temp_xml = "temp_xml"

    def __init__(self, database):
        self.database = database
        self.count_xml_tags = 0
        self.count_xml_files = 0
        self.total_xml_files = None
        self.tag_name = None
        self.object_tag_name = None
        self.file_bag_code = None
        self.start_time = None
        self.db_fields = {}
        self.today_string = utils.bag_date_today()
        # self.data_init = {'status': '', 'begindatum_geldigheid': '', 'einddatum_geldigheid': ''}
        self.data_init = {}

        if not os.path.exists(self.folder_temp_xml):
            os.makedirs(self.folder_temp_xml)

    def parse(self, tag_name):
        self.tag_name = tag_name
        self.count_xml_tags = 0

        if self.tag_name == 'Woonplaats':
            self.object_tag_name = tag_name
            self.file_bag_code = "9999WPL"
            self.data_init['geometry'] = None
            self.db_fields = {
                'id': ('identificatie', FIND_FIELD),
                'naam': ('naam', FIND_FIELD),
                'begindatum_geldigheid': ('beginGeldigheid', FIND_FIELD),
                'einddatum_geldigheid': ('eindGeldigheid', FIND_FIELD),
                'status': ('status', FIND_FIELD),
            }
            if config.parse_geometries:
                self.db_fields['geometry'] = ('posList', FIND_FIELD)

        elif self.tag_name == 'GemeenteWoonplaatsRelatie':
            self.object_tag_name = tag_name
            self.file_bag_code = "GEM-WPL-RELATIE"
            self.db_fields = {
                'begindatum_geldigheid': ('begindatumTijdvakGeldigheid', FIND_FIELD),
                'einddatum_geldigheid': ('einddatumTijdvakGeldigheid', FIND_FIELD),
                'status': ('status', FIND_FIELD),
                'woonplaats_id': (['gerelateerdeWoonplaats', 'identificatie'], FIND_NESTED_FIELD),
                'gemeente_id': (['gerelateerdeGemeente', 'identificatie'], FIND_NESTED_FIELD),
            }

        elif self.tag_name == 'OpenbareRuimte':
            self.object_tag_name = tag_name
            self.file_bag_code = "9999OPR"
            self.db_fields = {
                'id': ('identificatie', FIND_FIELD),
                'naam': ('naam', FIND_FIELD),
                'type': ('type', FIND_FIELD),
                'inactief': ('aanduidingRecordInactief', FIND_FIELD),
                'begindatum_geldigheid': ('beginGeldigheid', FIND_FIELD),
                'einddatum_geldigheid': ('eindGeldigheid', FIND_FIELD),
                'status': ('status', FIND_FIELD),
                'woonplaats_id': ('WoonplaatsRef', FIND_FIELD),
                'verkorte_naam': (['verkorteNaam', 'VerkorteNaamOpenbareRuimte', 'verkorteNaam'], FIND_NESTED_FIELD),
            }
        elif self.tag_name == 'Nummeraanduiding':
            self.object_tag_name = tag_name
            self.file_bag_code = "9999NUM"
            self.db_fields = {
                'id': ('identificatie', FIND_FIELD),
                'postcode': ('postcode', FIND_FIELD),
                'huisnummer': ('huisnummer', FIND_FIELD),
                'huisletter': ('huisletter', FIND_FIELD),
                'toevoeging': ('huisnummertoevoeging', FIND_FIELD),
                'begindatum_geldigheid': ('beginGeldigheid', FIND_FIELD),
                'einddatum_geldigheid': ('eindGeldigheid', FIND_FIELD),
                'status': ('status', FIND_FIELD),
                'openbare_ruimte_id': ('OpenbareRuimteRef', FIND_FIELD),
                'woonplaats_id': ('WoonplaatsRef', FIND_FIELD),
            }

        elif self.tag_name == 'Pand':
            self.object_tag_name = tag_name
            self.file_bag_code = "9999PND"
            self.data_init['geometry'] = None
            self.db_fields = {
                'id': ('identificatie', FIND_FIELD),
                'bouwjaar': ('oorspronkelijkBouwjaar', FIND_FIELD),
                'begindatum_geldigheid': ('beginGeldigheid', FIND_FIELD),
                'einddatum_geldigheid': ('eindGeldigheid', FIND_FIELD),
                'status': ('status', FIND_FIELD),
            }
            if config.parse_geometries:
                self.db_fields['geometry'] = ('posList', FIND_FIELD)

        elif self.tag_name == 'Verblijfsobject':
            self.object_tag_name = tag_name
            self.file_bag_code = "9999VBO"
            self.db_fields = {
                'id': ('identificatie', FIND_FIELD),
                'oppervlakte': ('oppervlakte', FIND_FIELD),
                'gebruiksdoel': ('gebruiksdoel', FIND_FIELD_MULTI),
                'pos': ('pos', FIND_FIELD),
                'begindatum_geldigheid': ('beginGeldigheid', FIND_FIELD),
                'einddatum_geldigheid': ('eindGeldigheid', FIND_FIELD),
                'status': ('status', FIND_FIELD),
                'pand_id': ('PandRef', FIND_FIELD_MULTI),
                'nummer_id': (['heeftAlsHoofdadres', 'NummeraanduidingRef'], FIND_NESTED_FIELD),
                'nevenadressen': (['heeftAlsNevenadres', 'NummeraanduidingRef'], FIND_NESTED_FIELD_MULTI),
            }

        elif self.tag_name == 'Ligplaats':
            self.object_tag_name = tag_name
            self.file_bag_code = "9999LIG"
            self.data_init['pos'] = None
            self.data_init['rd_x'] = None
            self.data_init['rd_y'] = None
            self.data_init['latitude'] = None
            self.data_init['longitude'] = None
            self.db_fields = {
                'id': ('identificatie', FIND_FIELD),
                'geometry': ('posList', FIND_FIELD),
                'inactief': ('aanduidingRecordInactief', FIND_FIELD),
                'begindatum_geldigheid': ('beginGeldigheid', FIND_FIELD),
                'einddatum_geldigheid': ('eindGeldigheid', FIND_FIELD),
                'status': ('status', FIND_FIELD),
                'nummer_id': (['heeftAlsHoofdadres', 'NummeraanduidingRef'], FIND_NESTED_FIELD),
            }

        elif self.tag_name == 'Standplaats':
            self.object_tag_name = tag_name
            self.file_bag_code = "9999STA"
            self.data_init['pos'] = None
            self.data_init['rd_x'] = None
            self.data_init['rd_y'] = None
            self.data_init['latitude'] = None
            self.data_init['longitude'] = None
            # self.data_init['geometry'] = ''

            self.db_fields = {
                'id': ('identificatie', FIND_FIELD),
                'geometry': ('posList', FIND_FIELD),
                'inactief': ('aanduidingRecordInactief', FIND_FIELD),
                'begindatum_geldigheid': ('beginGeldigheid', FIND_FIELD),
                'einddatum_geldigheid': ('eindGeldigheid', FIND_FIELD),
                'status': ('status', FIND_FIELD),
                'nummer_id': (['heeftAlsHoofdadres', 'NummeraanduidingRef'], FIND_NESTED_FIELD),
            }
        else:
            raise Exception("Tag name not found")

        utils.print_log(f'start: parse {self.tag_name}')

        self.__unzip_xml()

        utils.print_log('convert XML files to DuckDB')
        self.__parse_xml_files()

        time_elapsed = utils.time_elapsed(self.start_time)
        utils.print_log(f'ready: parse XML {self.tag_name} | {time_elapsed} '
                        f'| XML nodes: {self.count_xml_tags:,d}')

        utils.empty_folder(self.folder_temp_xml)

    def __unzip_xml(self):
        utils.empty_folder(self.folder_temp_xml)

        file_zip = utils.find_file('temp', self.file_bag_code, 'zip')

        utils.print_log('unzip ' + file_zip)
        utils.unzip_files_multithreaded(file_zip, self.folder_temp_xml)

    def __parse_xml_files(self):

        post_sql = None
        match self.tag_name:
            case 'Woonplaats':
                save_function = self.database.save_woonplaats
            case 'GemeenteWoonplaatsRelatie':
                save_function = self.database.save_gemeente_woonplaats
            case 'OpenbareRuimte':
                save_function = self.database.save_openbare_ruimte
                if config.use_short_street_names:
                    post_sql = 'UPDATE openbare_ruimten SET naam=verkorte_naam WHERE verkorte_naam is not NULL'
            case 'Nummeraanduiding':
                save_function = self.database.save_nummer
            case 'Pand':
                save_function = self.database.save_pand
            case 'Verblijfsobject':
                save_function = self.database.save_verblijfsobject
                post_sql = "UPDATE verblijfsobjecten SET lon_lat=st_point(longitude, latitude) WHERE longitude is not NULL and latitude is not NULL"
            case 'Ligplaats':
                save_function = self.database.save_ligplaats
                post_sql = "UPDATE ligplaatsen SET lon_lat=st_point(longitude, latitude) WHERE longitude is not NULL and latitude is not NULL"
            case 'Standplaats':
                save_function = self.database.save_standplaats
                post_sql = "UPDATE standplaatsen SET lon_lat=st_point(longitude, latitude) WHERE longitude is not NULL and latitude is not NULL"
            case _:
                raise Exception(f'No save function found for tag_name "{self.tag_name}"')

        xml_files = utils.find_xml_files(self.folder_temp_xml, self.file_bag_code)
        files_total = len(xml_files)
        self.total_xml_files = files_total
        self.count_xml_files = 0

        self.start_time = time.perf_counter()

        workers_count = config.cpu_cores_used

        futures = []
        # Multi-threading and batch
        # use ceil instead of round to prevent zero batch size
        batch_size = math.ceil(files_total / workers_count)

        # Multi-threading. One XML file per executor.
        pool = ProcessPoolExecutor(workers_count)
        for file_xml in xml_files:
            future = pool.submit(
                parse_xml_file,
                file_xml,
                self.tag_name,
                self.data_init,
                self.object_tag_name,
                self.db_fields)
            futures.append(future)

        for future in futures:
            result = future.result()
            count_file_xml = result['count']

            save_function(result['data'])

            self.count_xml_files += 1
            self.count_xml_tags += count_file_xml
            self.__update_xml_status()

        wait(futures)
        self.__update_xml_status(True)
        if post_sql:
            utils.print_log(f"Post processing {self.tag_name}")
            self.database.post_process(post_sql)


    def add_gemeenten_into_woonplaatsen(self):
        if (not config.active_only):
            utils.print_log('gemeente_id is only added to woonplaatsen if active_only=True in config', True)
            return
        self.database.add_gemeenten_to_woonplaatsen()

    def __update_xml_status(self, final=False):
        if (final or
                (self.gui_time is None) or
                (time.perf_counter() - self.gui_time > 0.5)):

            self.gui_time = time.perf_counter()
            self.elapsed_time = self.gui_time - self.start_time
            tags_per_second = round(self.count_xml_tags / self.elapsed_time)
            time_elapsed_text = utils.time_elapsed(self.start_time)

            bar_text = f" {time_elapsed_text} | XML nodes: {self.count_xml_tags:,d} | per second: {tags_per_second:,d}"
            utils.print_progress_bar(self.count_xml_files, self.total_xml_files, bar_text, final)
