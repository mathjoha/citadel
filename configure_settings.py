# coding=<utf-8>
from settings import settings
import argparse
import re
import os


def usable_path(path):
    """Quickly checkinf if the path can be used."""
    if path == '' or type(path) is not str:
        return False
    if not path.endswith('.sqlite3'):
        path += '.sqlite3'
    if len(path) < 9:
        return False
    elif os.path.exists(path):
        print(f'File: {path} already exists')
        return False
    else:
        try:
            with open(path, 'w') as f:
                f.write('')
                pass
            os.remove(path)
            return path
        except FileNotFoundError:
            print(f'Could not find path: {path}')
            return False


if __name__ == '__main__':
# later: (10) add a flag for wikipedia, 0: do not use, 1+ the number of Qs to resolve before commiting to db.
    parser = argparse.ArgumentParser(
        description='This script will help you set up the toponym database.'
                    'All these settings will be stored in .yaml format, '
                    'which is in a plain text format and can be edited with'
                    'just about any text editor. \n'
                    'Since your server token will be stored in this file'
                    ', anyone with access to the file can control your'
                    'application. So share it at your own risk.')

    # TODO : Add some examples here. -- also add these to the README.md
    parser.add_argument('-d', '--db_path', metavar='database_path', type=str,
                        const='s', nargs='?',
                        help='Path to where the database should be.')
    parser.add_argument('-t', '--token', metavar='server_token', type=str,
                        const='', nargs='?',
                        help='Token for connecting to Anvil.Works GUI')
    parser.add_argument('-l', '--languages', metavar='languages', type=str,
                        nargs='*', default=set(),
                        help='The ISO 639 code(s) for relevant language(s).\n'
                        'If left blank, all languages will be imported.')
    parser.add_argument('-c', '--countries', metavar='countries', type=str,
                        nargs='+',
                        help='The ISO 3166 code(s) for countrie(s) of interest'
                        )
    parser.add_argument('-a', '--adjacents', metavar='adjacents', type=str,
                        nargs='*', default=set(),
                        help='Add adjacent countries with locations that you'
                        'suspect may be in your input dataset.'
                        'These names will be mapped to the same single point'
                        'to help filter out them from.')
    parser.add_argument('-n', '--num-rows', metavar='num_rows', type=int,
                        nargs='?', default=1e100,
                        help='The number of rows of toponyms to batch add to '
                        'database, may be necessary for machines with less '
                        'memory.')
    parser.add_argument('-w', '--wiki-rows', metavar='wiki_rows', type=int,
                        nargs='?', default=500,
                        help='Set the size of the query to wikidata for more '
                        'topnym variants. Defaults to 500, 0 turns this off.')

    args = parser.parse_args()

    settings.num_rows = args.num_rows

    db_path = args.db_path
    token = args.token
    # using sets to make sure that no code is entered twice.
    countries = set(_.upper() for _ in args.countries)
    adjacents = set(_.upper() for _ in args.adjacents) - countries

    languages = set(args.languages)

    if countries is None:
        raise ValueError('No country has been selected.')
    else:
        settings.countries = countries

    lang_pattern = re.compile(r'[a-z]+')

    if languages is None:
        print('No languages have been entered.')
        languages = input('Please enter ISO-code for relevant languages, or '
                          'just press enter to  proceed with all languages: ')
        if languages == '':
            languages = None
        else:
            languages = lang_pattern.findall(languages)
    else:
        settings.languages = languages

    if adjacents is None:
        print('No adjacents have been entered.')
        adjacents = input('Please enter ISO-code for relevant adjacents, or '
                          'just press enter to  proceed with all adjacents: ')
        if adjacents == '':
            adjacents = None
        else:
            adjacents = lang_pattern.findall(adjacents)
    else:
        settings.adjacents = adjacents

    if not usable_path(db_path):
        print(f'Database path is invalid: {db_path}')
        while not (db_path := usable_path(input('Please enter a valid path to '
                                                'continue: '))):
            pass
    db_path = usable_path(db_path)

    settings.database_path = db_path

    if token is None or token == '':
        print('Token has not been set, and will be required to access the GUI.'
              '\n Enter the token below to save it now or manually enter it '
              'into "toponym_settings.yaml" later')
        token = input('Enter new token:')
        if len(token) < 10:
            token = 'Replace this text with your server token'
    settings.server_token = token

    settings.wiki_rows = min(0, abs(args.wiki_rows))

    settings.save()
    print('Settings saved.')
