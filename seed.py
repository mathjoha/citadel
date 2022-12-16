# coding=<utf-8>

import logging
import os
from settings import settings
# This may not be a good idea
from toponym_main import add_source, add_position
from operations import execute, preprocess_toponym, add_toponym_list
from datetime import datetime
from collections import namedtuple
import wget
import zipfile
from tqdm import tqdm
from collections import defaultdict
from wiki_operations import fetch_base_item
from operations import resolve_wiki_queue, wiki_queue_cleanup

from initiate_schema import create_tables

geoname = namedtuple('geoname',
                     ['geonameid', 'name', 'asciiname', 'alternatenames',
                      'latitude', 'longitude', 'feature_class', 'feature_code',
                      'country_code', 'cc2', 'admin1_code', 'admin2_code',
                      'admin3_code', 'admin4_code', 'population', 'elevation',
                      'dem', 'timezone', 'modification_date'])


altname = namedtuple('altname',
                     ['alternateNameId',
                      'geonameid',
                      'language',
                      'name',
                      'isPreferredName',
                      'isShortName',
                      'isColloquial',
                      'isHistoric',
                      'used_from',
                      'used_to',
                      ])

wikiname = namedtuple('wikiname',
                      ['comment', 'name', 'language'])

geonames_dir_path = 'geonames'


def add_known_toponym(source, raw_name, position_id,
                      asciiname=None, language='-'):

    tokens, asciiname, asciitokens, pattern = preprocess_toponym(
            raw_name, asciiname)

    execute('INSERT INTO toponym (name, source_fk, asciiname, tokens, '
            'asciitokens, pattern, position_fk, toponym_created_date, '
            'language, comment)'
            'values (:name, :source, :asciiname, :tokens, '
            ':asciitokens, :pattern, :position_fk, datetime("NOW"),'
            ':language, "")',
            {'name': raw_name,
             'source': source,
             'asciiname': asciiname,
             'tokens': tokens,
             'asciitokens': asciitokens,
             'pattern': pattern,
             'position_fk': position_id,
             'language': language})


def get_geonames_txt(file):
    """Makes sure the relevant txt file exists and returns its path"""
    txt_path = os.path.join(geonames_dir_path, file + '.txt')
    if not os.path.exists(txt_path):
        get_geonames_zip(file)
    return txt_path


def get_geonames_zip(zip_file):
    """Makes sure the GeoNames zip is downloaded and extracts its content"""
    zip_path = os.path.join(geonames_dir_path, zip_file+'.zip')
    if not os.path.exists(zip_path):
        url = f'http://download.geonames.org/export/dump/{zip_file}.zip'
        print(f'\tDownloading: {url}')
        downloaded = wget.download(url)
        os.rename(downloaded, zip_path)

    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(geonames_dir_path)


def seed_alt_names():

    """Seeds all the alternate names"""

    alt_names_path = get_geonames_txt('alternateNamesV2')

    geo_al_source = 'GeoAlt'
    add_source(name=geo_al_source, comment='alternateNamesV2',
               year=datetime.now().year)
    wiki_source = 'WikDat'
    add_source(name=wiki_source, comment='wikidata',
               year=datetime.now().year)

    with open(alt_names_path, 'r', encoding='utf8') as f:

        names_dict = defaultdict(list)
        wiki_ndict = defaultdict(list)
        for i, name in enumerate(tqdm(f.readlines(), desc='Preprocess rows')):

            split_line = name.split('\t')

            if len(split_line) != 10:
                print('\a')
                logging.critical(f'One of the line of the {alt_names_path} '
                                 'was not exatcly 10 columns wide. ')
                raise IOError(f'{alt_names_path} may be corrupted.')

            alt_row = altname(*split_line)

            if alt_row.language == 'link' and 'wikipedia' in alt_row.name:
                wiki_ndict[alt_row.geonameid].append(alt_row)
                continue
            elif (alt_row.language not in settings.languages and
                  settings.languages != {}):
                if alt_row != '':
                    continue

            names_dict[alt_row.geonameid].append(alt_row)

            if len(names_dict) >= settings.num_rows:
                used = process_portion(names_dict, source_name='GeoAlt')
                names_dict = defaultdict(list)

                process_wiki_portion(wiki_ndict, source_name='WikDat',
                                     used_geonames=used)
                wiki_ndict = defaultdict(list)
                del used

    # this is where it seems to stop...

    # recording the last names
    used = process_portion(names_dict, source_name='GeoAlt')
    if process_wiki_portion(wiki_ndict, source_name='WikDat',
                            used_geonames=used):
        wiki_queue_cleanup()


def process_wiki_portion(names_dict, source_name, used_geonames):
    """Resolves a portion of the Wikipedia links and fetches toponyms"""

    wiki_records = []
    for geoid in used_geonames:
        for row in names_dict[geoid]:
            # title = fetch_title(row.name)
            # if title is None:
            #     continue
            base_item = fetch_base_item(row.name)

            if type(base_item) is str:
                base_item = base_item.lower()

            # if base_item is not None:
            wiki_records.append((base_item, geoid,
                                 source_name, row.name, False))

    if len(wiki_records) > 0:
        execute('INSERT INTO wiki_queue (wiki_id, position_fk, source_fk, '
                'title, processed) values (?, ?, ?, ?, ?)',
                values=wiki_records,
                many=True, status='Adding wikidata to queue')

    return resolve_wiki_queue()


def process_portion(names_dict, source_name):
    """Records a portion of the toponyms"""
    used_geonames = [str(_[0]) for _ in execute(
            'SELECT position_id from position')]

    alt_names = []

    for geonameid in used_geonames:
        for row in names_dict[geonameid]:

            tokens, asciiname, asciitokens, pattern = preprocess_toponym(
                row.name)
            try:
                comment = row.comment
            except AttributeError:
                comment = ''

            alt_names.append((geonameid,
                             source_name,
                             row.name,
                             asciiname,
                             pattern,
                             tokens,
                             asciitokens,
                             row.language,
                             comment)
                             )
    if len(alt_names) > 0:
        add_toponym_list(alt_names)

    return used_geonames


def seed_admin():
    """ Adds all the names from used admin2 codes from geonames"""
    admin2_file = os.path.join(geonames_dir_path, 'admin2Codes.txt')
    if not os.path.exists(admin2_file):
        wget.download('http://download.geonames.org/export/dump/'
                      'admin2Codes.txt')
        os.rename('admin2Codes.txt', admin2_file)

    with open(admin2_file, 'r', encoding='utf8') as f:
        admins = f.readlines()
    admins = [_.split('\t')[:2] for _ in admins if _[:2] in settings.countries]
    execute('INSERT INTO parent_region (parent_id, name) values (?, ?)',
            values=admins, many=True)


def seed_positions():
    """Seeding positions table from GeoNames and its base toponym"""

    add_source(name='none', comment='For linking foreign positions', year=2022)
    add_position(position_id=0,
                 longitude=0,
                 latitude=0,
                 source="none",
                 parent_id='none')

    countries = [(c, True) for c in settings.countries]
    countries += [(c, False) for c in settings.adjacents]

    for country, main in tqdm(countries, total=len(countries),
                              desc='Seeding positions by country'):
        positions = []
        names = []
        txt_path = get_geonames_txt(country)
        source_name = f'GeoN{country}'.lower()
        add_source(source_name, f'GeoNames {txt_path}',
                   datetime.now().year)
        # print(txt_path)
        with open(txt_path, 'r', encoding='utf8') as f:
            s = f.read().split('\n')
            # for line in f.readlines():
        for line in tqdm(s):
            if line == '':
                continue
            geo_row = geoname(*line.split('\t'))

            if geo_row.feature_class == 'A':
                # store admin data and fetch names.
                continue

            if main:

                # assembling admin2 codes
                admin_code = '.'.join((country, geo_row.admin1_code,
                                       geo_row.admin2_code))
            else:
                admin_code = '0'

            positions.append(
                    (geo_row.geonameid,
                     source_name,
                     geo_row.latitude,
                     geo_row.longitude,
                     admin_code))

            tokens, asciiname, asciitokens, pattern = preprocess_toponym(
                geo_row.name)

            names.append([geo_row.geonameid,
                          source_name,
                          geo_row.name,
                          asciiname,
                          pattern,
                          tokens,
                          asciitokens,
                          '', 'GeoNames-Default']
                         )

        try:
            _ = execute('INSERT into position (position_id, source_fk, '
                        'latitude, '
                        'longitude, '
                        'parent_fk, '
                        'position_created) values'
                        '(?, ?, ?, ?, '
                        ':parent_fk, datetime("NOW"))',
                        values=positions,
                        many=True)
            print(f'{len(positions)=}')
            del positions
        except Exception as e:
            print(country)

            raise e

        add_toponym_list(names)


def seed_tables():
    """Seeding all tables with data from GeoNames and WikiData"""

    os.makedirs(geonames_dir_path, exist_ok=True)

    seed_admin()

    seed_positions()

    seed_alt_names()


if __name__ == '__main__':
    logging.basicConfig(
        filename='seeding.log',
        level=logging.DEBUG,
        format='%(asctime)s %(message)s',
        datefmt='%Y-%m-%d @ %H:%M:%S ',
        # encoding='utf8',
        force=True
        )

    if not os.path.exists(settings.database_path):
        # initiate schema
        create_tables()

        # initiate seed
        seed_tables()
    else:
        print('Database already exists')
        wiki_queue_cleanup()
