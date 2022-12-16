# rewrite this as a class?
from settings import settings
import logging
import sqlite3
from anyascii import anyascii
from stopwords import get_stopwords
import re
import inspect
from wiki_operations import get_wiki_names
from time import sleep

# some globals
stops = set()
for lang in settings.languages:
    stops |= set(get_stopwords(lang))


# Enable the logging of queries.
sqlite3.enable_callback_tracebacks(True)


# Handling all the processing in one place to enable betterlogging
# But most importantly, this makes it easier to later change DB type.
# It started from not being able to use a single connection throughout
# the server
def execute(query, values=(), status='', many=False):
    """Wrapper function for sqlite3 connection

    Takes:
        query - str
        values - typically dict or tuple
        status - A brief explanation to explain the operation in the log
        many - bool, whether the executemany function should be used

    Connects to the database from the settings, executes the query and
    logs the process.

    Returns:
        List of results from select query, None otherwise
    """

    if status == '':
        status = f'Called from: {inspect.stack()[1][1:4]}'

    try:
        # Since the connection cannot be shared between threads, each
        # query needs its own temporary connection.
        with sqlite3.connect(settings.database_path) as conn:
            # For debugging queries
            conn.set_trace_callback(logging.debug)
            cur = conn.cursor()
            if len(values) == 0:
                cur.execute(query)
            elif many:
                status += 'Many: ' + str(len(values))
                cur.executemany(query, values)
            else:
                status += str(values)
                cur.execute(query, values)
            results = cur.fetchall()

        if type(results) is None:
            results_len = 'None'
        else:
            results_len = len(results)

        status += f' Executed with {results_len=} results'
        if any(_ in query.lower() for _ in ['insert', 'update', 'delete']):
            conn.commit()
        return results
    except Exception as e:
        # logging query and error for debugging
        logging.debug(f'{query=} led to {e=}')
        print(f'{query=}, {values=}, {many=} led to {e=}')
        raise e
    finally:
        logging.info(f'executed: {query=} -- {status=}')

    return


def add_toponym_list(names, status='Adding toponyms'):
    """Adds a list of new toponyms to the toponym table"""
    _ = execute('INSERT into toponym (position_fk, source_fk, name, '
                'asciiname, pattern, tokens, asciitokens, '
                ' language, comment, toponym_created_date'
                ') values'
                '(?, ?, ?, ?, ?, ?, ?, ?, ?, datetime("NOW"))',
                values=names,
                many=True,
                status=status)


def preprocess_toponym(raw_name, asciiname=None):
    """Precalculate ascii, pattern, tokens and ascii tokens for a toponym"""
    if asciiname is None:
        asciiname = anyascii(raw_name)
    pattern = str(''.join(utf if utf == char else "_" for utf, char in
                  zip(raw_name, asciiname)))
    tokens = tokenize(raw_name)
    asciitokens = tokenize(asciiname)

    return tokens, asciiname, asciitokens, pattern


# generic comment function
def write_comment(comment, table, field, value):
    """Generic function for writing to the beginning of comment fields

    Takes:
        comment - str - the addition to the comment
        table - name of the table to update
        field - the field to compare the value against
        value - the value to select rows

    Adds the supplied 'comment' to the beginning of the comment field for the
    matching table, field and value.
    """
    _ = execute(f'UPDATE {table} set comment = :comment || " \n " || comment '
                f'where {field} == :value ',
                {'comment': comment.strip(),
                 'value': value})
    return _


def tokenize(name):
    """Tokenize names, remove stopwords, sort and join to text"""

    # Split the name into tokens
    tokens = [token.lower() for token in re.findall(r'\w+', name)]
    # Remove stopwords, add commas and join by spaces
    # Sorting them so that seemlingly different names become perfect matches
    sorted_tokens = sorted([f',{token},' for token in tokens if
                            token not in stops])
    if len(sorted_tokens) == 0:
        return '_'
    return ' '.join(sorted_tokens)


def connect_toponym(toponym_id, position_fk, comment):
    """
    Takes:
        toponym_id
        position_fk
        comment

    Registers the data in the toponym_id's row:
        Setting its position_fk t0 the input
        Concatenating the supplied comment with the existing one.
    """
    query = 'update toponym set position_fk = :position_fk, '\
            'comment = :comment || " \n " || comment '\
            'where toponym_id == :toponym_id'
    values = {'position_fk': position_fk, 'comment': comment,
              'toponym_id': toponym_id}
    status = f'Connecting {toponym_id=} to {position_fk=}'
    return execute(query, values=values, status=status)


def find_mappable_suggestions(target_id=None):
    """Generator for finding suggestions that are ready to be mapped"""
    values = {}
    query = 'select added_toponym_fk, position_fk from suggestion join '\
            'toponym on toponym_id == stable_toponym_fk '\
            'where outcome is NULL and added_toponym_fk not in '\
            '(select added_toponym_fk from suggestion where outcome is TRUE) '
    if target_id is not None:
        query += ' and added_toponym_fk == :target '
        values['target'] = target_id
    query += 'group by '\
             'added_toponym_fk having count(distinct position_fk) == 1'
    for result in execute(query, values=values, status='Mappable suggestions'):
        yield result


def merge_suggestions(target_id=None):
    """Assigns a toponym's position to the consensus of suggestions"""
    comment = f'No suggestions for {target_id}'
    for toponym_id, position_id in find_mappable_suggestions(target_id):
        positions = execute('select name, suggestion.comment '
                            'from suggestion join toponym on '
                            'toponym_id == stable_toponym_fk '
                            'where added_toponym_fk == :target',
                            values={'target': toponym_id})
        comment = 'All remaining suggestions pointed to the same position: \n'
        for position in positions:
            comment += f'{position[0]} ({position[1]}) \n '

        # connect toponym to position
        connect_toponym(toponym_id, position_id, comment)

        # Accept all suggestions - and comment
        _ = execute('update suggestion set outcome = TRUE, '
                    'comment = "Automatically Approved \n " || comment where '
                    'added_toponym_fk = :toponym_id',
                    values={'toponym_id': toponym_id})

    if target_id is not None:
        return comment


def resolve_wiki_queue(n_rows=settings.wiki_rows):
    """Fetching appropriate toponyms from WikiData based on the queue

    Takes:
        n_rows (int) - The number of rows from the queue to process.

    Queries the wiki queue for 1 <= n_rows <= 500 entries, use their links
    to query WikiData for toponyms in any of the languages from the settings.

    Adds all entries to the toponyms table, linked to the appropriate position
    and marks the queue item as processed.


    """

    if n_rows > 500:
        n_rows == 500
    elif n_rows < 0:
        n_rows = 1
    elif n_rows == 0:
        return

    query = 'select wiki_id, position_fk, source_fk from wiki_queue where '\
            ' processed == FALSE and wiki_id is not NULL '\
            f'order by random() limit {n_rows}'
    queue = sorted(execute(query, status='Fetching wiki queue'))

    identifiers = {q: (position, source) for q, position, source in queue if
                   q.startswith('q')}
    if len(identifiers) > 0:
        result_dict = get_wiki_names(identifiers.keys(), settings.languages)

        for q, (position, source) in identifiers.items():
            rows = []
            for toponyms, language in result_dict[q]:
                for toponym in toponyms.split('___'):
                    processed_toponym = preprocess_toponym(toponym)
                    tokens, asciiname, asciitokens, pattern = processed_toponym
                    rows.append((position,
                                 'WikDat',
                                 toponym,
                                 asciiname,
                                 pattern,
                                 tokens,
                                 asciitokens,
                                 language,
                                 f'WikiData: {q}',
                                 ))

            if len(rows) > 0:
                add_toponym_list(rows)

            # recording the Q as done.

            q_done_query = 'update wiki_queue set processed = TRUE where '\
                           'wiki_id == :processed_q'

            execute(q_done_query, values={'processed_q': q},
                    status=f'{q} Processed.')

        return True
    else:
        return False


def wiki_queue_cleanup():
    """Processes the WikiData queue, one batch at a time"""

    while resolve_wiki_queue():
        logging.info('Taking care of the last wikipedia toponyms '
                     f'{settings.wiki_rows} toponyms at at time')
        sleep(10)


if __name__ == '__main__':
    wiki_queue_cleanup()
