# coding=<utf-8>
from initiate_schema import create_tables
from settings import settings
import anvil.server
import logging
import sqlite3
# from anyascii import anyascii
# from stopwords import get_stopwords
# from nltk.corpus import stopwords
import matchers
from operations import execute
from operations import preprocess_toponym
from operations import write_comment
from operations import connect_toponym
from statistics import mean
from anyascii import anyascii
from collections import namedtuple
from collections import Counter
from geopy.distance import great_circle

DisambigTuple = namedtuple('DisambigTuple',
                           ['added_toponym_id', 'newname', 'source_fk',
                            'oldid', 'oldname',
                            'suggestioncomment',
                            'pcomment', 'paltnames', 'adminname', ])


def position_distance(pos1, pos2):
    return great_circle(pos1, pos2).km


# add source
@anvil.server.callable
def add_source(name, comment, year):
    """
    Takes:
        name (6chars)
        comment (str)
        year (int4)
    and inserts them into the table

    Since sqllite lacks this level of type control, we need to control for it.

    Returns:
        the execution outcome
    """

    assert len(str(year)) == 4
    assert type(name) is str

    _ = execute('INSERT INTO source (name, comment, year, source_date_edited) '
                'values (?, ?, ?, datetime("now"))',
                values=(name.lower(), comment, year))
    return _


# comment source
@anvil.server.callable
def comment_source(source_name, comment):
    """
    Takes:
        source pk - string (char6)
        comment - string to append to comment
    """
    _ = write_comment(comment=comment, table='source', field='name',
                      value=source_name)
    return _


def source_available(source):
    """Checks if the supplied source name is available or taken"""
    _ = execute('select name from source where name == :source',
                {'source': source}, status=f'Checking {source} availability')
    return len(_) == 0


# add toponym
@anvil.server.callable
def add_toponym(source, raw_names):
    """Adds a list of toponyms from a source to the toponyms table
    Takes:
        source (str) - 6-10 chars is the unique identifier of the source
        raw_names - a list of tuples expected to contain:
            toponym name - as is, possibly with transcription errors
            lang - '' or 2-character language ISO 639 code
            position - '' or a fk to the position table
    """

    # check that source exists:
    if source_available(source):
        raise ValueError('Source does not exists.')

    raw_names_in_count = len(raw_names)

    # for some reason the passed tuples are changed to lists.
    raw_names = [(n.strip(), lang, pos) for n, lang, pos in raw_names]

    if type(raw_names) is str:
        raw_names = {raw_names, source}
        # recounting set
        raw_names_in_count = len(raw_names)
    else:
        raw_names = set(raw_names)

    raw_names_uniq_count = len(raw_names)

    used_names = set(execute('SELECT name, language, position_fk from toponym '
                             'where source_fk == :source',
                             {'source': source}))
    raw_names -= used_names

    raw_names_entered_count = len(raw_names)

    logging.debug(f'Raw names: {raw_names_in_count}, {raw_names_uniq_count}, '
                  f'{raw_names_entered_count}')

    values = []
    for raw_name, language, position in raw_names:

        tokens, asciiname, asciitokens, pattern = preprocess_toponym(
            raw_name)
        values.append((raw_name, source, asciiname, tokens, asciitokens,
                       pattern, position, language))

    q_marks = q_marker(values[0])

    _ = execute('INSERT INTO toponym (name, source_fk, asciiname, tokens, '
                'asciitokens, pattern, position_fk, language, '
                'toponym_created_date, comment) '
                'values (' + q_marks + ', datetime("now"), "")',
                values=values,
                many=True)
    return len(values)


# comment toponym
@anvil.server.callable
def comment_toponym(toponym_id, comment):
    """Appends a new string to the beginning of a toponym comment"""
    _ = write_comment(comment=comment, table='toponym', field='toponym_id',
                      value=toponym_id)
    return _


# add position
@anvil.server.callable
def add_position(position_id, source, latitude, longitude,
                 # precision,
                 # abandoned,
                 # parent_name,
                 parent_id,
                 comment=None):
    """Creates a record of a position"""

    if source_available(source=source):
        raise ValueError(f'{source} is not available')

    # feature class and or name may be necessary
    try:
        _ = execute('INSERT into position (position_id, source_fk, latitude, '
                    'longitude, '
                    'parent_fk, comment, '
                    'position_created) values'
                    '(:position_id, :source, :latitude, :longitude, '
                    ':parent_fk, :comment, datetime("NOW"))',
                    {'position_id': position_id, 'source': source,
                     'latitude': latitude, 'longitude': longitude,
                     # 'abandoned':abandoned,
                     # 'parent_name': parent_name,
                     'parent_fk': parent_id,
                     'comment': comment})
    except sqlite3.IntegrityError as e:
        logging.warning(f'Duplicate coordinates found for: {position_id=}, '
                        f'{source=}, {latitude=}, {longitude=}, {parent_id=}\n'
                        f'{e}')
    return _


@anvil.server.callable
def change_coordinates(position_id, latitude, longitude):
    """Change a positions coordinates"""
    execute('update position set latitude = :latitude, longitude = :longitude'
            ' where position_id == :position_id', status='Changing coords',
            values={'position_id': position_id,
                    'latitude': latitude, 'longitude': longitude})


# comment position
@anvil.server.callable
def comment_position(comment, position_id):
    """Append comment to the beginning of position comment"""
    _ = write_comment(comment=comment, table='position', field='position_id',
                      value=position_id)
    return _


def q_marker(items):
    """Creates a comma separated ? with the same len as supplied iterable"""
    return ', '.join('?' for _ in items)


@anvil.server.callable
def start_matcher(source=None):
    """Initiate the matching process in the background """
    task = anvil.server.launch_background_task('matcher', source)
    return task


@anvil.server.callable
def get_existing_matcher():
    """"Fetches existing matchers, if they exist"""
    return [t for t in anvil.server.list_background_tasks() if
            t.get_task_name() == 'matcher']


@anvil.server.callable
def kill_matcher(task):
    """Kills the supplied background task"""
    task.kill()


@anvil.server.background_task
def matcher(source_fk=None):
    """Background task for the auto matching process"""

    # making the matcher object that manages the matching process.
    matcher = matchers.matcher(source_fk, execute)

    try:
        out_message = 'No matches found yet'

        for rnd_c, last, prev, iterator, message in matcher.long_matching():
            if message.endswith('_match'):
                out_message = f'{message} on {iterator}/{prev} round #{rnd_c}'
            anvil.server.task_state['round_count'] = rnd_c
            anvil.server.task_state['last'] = last
            anvil.server.task_state['prev_last'] = prev
            anvil.server.task_state['iterator'] = iterator
            anvil.server.task_state['multi_string'] = out_message
    except Exception as e:
        print(e)
        raise e


@anvil.server.callable
def start_nemo_list(*toponym_id):
    task = anvil.server.launch_background_task('make_nemo_list', *toponym_id)
    return task


@anvil.server.background_task
def make_nemo_list(*toponym_id):
    make_nemo(*toponym_id)


def make_nemo(*toponym_id):
    matcher = matchers.Nemo()

    matcher.top_10(*toponym_id)


@anvil.server.callable
def match_one_wait(toponym_id, source_fk):
    matcher = matchers.matcher(source_fk, execute)

    return matcher.run_all_matchers(toponym_id, suggest=True)[0]


@anvil.server.callable
def toponym_data(toponym_ids):
    """Fetching toponym plus position data for editors from toponym_ids list"""
    results = []

    for i, toponym_id in enumerate(toponym_ids):
        res = execute('select toponym_id, name, t.source_fk, position_id, '
                      'latitude, longitude, t.comment, p.comment, p.source_fk '
                      'from toponym as t left join position as p on '
                      'position_id == position_fk where '
                      'toponym_id == :toponym_id',
                      values={'toponym_id': toponym_id[0]})
        if type(res) is list:
            results.append(res[0])

    if len(results) == 0:
        return('No toponym data found.')

    results = [{'id':toponym_id, 'name':name, 'source':source, 'p_id':pos,
                'lat':lat, 'lng':lng, 'comment':comment, 'p_comment':p_comment,
                'p_source': p_source} for
                toponym_id, name, source, pos, lat, lng, comment, p_comment,
                p_source in results]
    return results

# TODO: (70) server.call('browser', 'positions')
@anvil.server.callable
def browser(table, filters={}, page=1):
    """A generic function for retreiving subsets from tables to display in app
    Takes:
        table (str) - the name of the table to retreive data from
        filters (dict) optional - information for which fields and values to
            filter the query on
        page (int) - which of the batches of 20 hits to return to the server.

    Returns:
        A list containing the selection of rows from the supplied table
        iltered by the supplied filters.

    """
    print(table, filters, page)
    lower = 20*(page-1)
    upper = page*20
    if table == 'toponym':
        values = {}

        if filters['source'] == 'All':
            source_filter = ' length(t.source_fk) > 6 '
        else:
            source_filter = ' t.source_fk == :source'
            values['source'] = filters['source']

        if filters['name'] in ('edit', '') or filters['name'] is None:
            name = ''
            order = ' order by toponym_edited desc '
        else:
            name = filters['name']
            name = f' and  asciiname like :name '
            order = ' order by name '
            values['name'] = filters['name']

        if filters['toponym_id'] != '':
            toponym = ' and toponym_id == :toponym_id'
            values['toponym_id'] = filters['toponym_id']
        else:
            toponym = ''

        if filters['position_fk'] != '':
            position = ' and position_fk like :position_fk '
            values['position_fk'] = '%'+filters['position_fk']+'%'
        else:
            position = ''

        if filters['p_source'] != '':
            p_source = ' and p.source_fk like :p_source '
            values['p_source'] = '%' + filters['p_source'] + '%'
        else:
            p_source = ''

        id_query = 'select toponym_id from toponym as t join position as p '\
                   'on position_id == position_fk where '
        id_query += source_filter + name + toponym + position + p_source
        id_query += order

        result = execute(id_query, values=values)
        if len(result) == 0:
            return 'Nothing found'
        return result

    elif table == 'position':
        result = execute('select position_id, source_fk, longitude, latitude,'
                         ' parentq, parent_name from position'
                         f'offset {lower} limit 100')
        if result is not None:
            return [{'id': idx, 'source': source, 'long': lng, 'lat': lat,
                     'abandoned': abandoned, 'parentq': parentq,
                     'parent_name': parent_name} for idx, source, lng, lat,
                        abandoned, parentq, parent_name in result]

    elif table == 'attraction':
        result = execute('select id, position, type, source from attraction')
        if result is not None:
            return [{'id': idx, 'position': pos, 'type': attr_type,
                    'source': source} for idx, pos, attr_type, source in result]

    elif table == 'languages':
        pass

    elif table == 'sources':

        result = execute('select name, comment, year from source')
        if result is not None:
            return [{'name': name, 'comment': comment, 'year': year} for name,
                    comment, year in result]

    elif table == 'nemo':
        result = execute('select toponym_id, name, language, source_fk, '
                         'comment from toponym where position_fk is null and '
                         'toponym_id not in (select added_toponym_fk from '
                         'suggestion where outcome is NULL or outcome is TRUE)')
        if result is not None:
            return [{'id': idx, 'name': name, 'language': language,
                     'source': source, 'comment': comment} for idx, name,
                     language, source, comment in result[lower:upper]]

    return 'Nothing found'


@anvil.server.callable
def fetch_sources(user_sources=True):
    """Fetching the source data for storage in anvil table"""

    query = 'select name, comment, year from source '
    if user_sources:
        query += 'where length(name) > 6'
    o = execute(query)

    return o


@anvil.server.callable
def declare_foreign(toponym_id):
    """Connects a toponym to the dummy point of (0,0)"""
    execute('update toponym set position_fk = 0, '
            'comment = "Declared irrelevant " || comment '
            'where toponym_id == :target',
            values={'target': toponym_id})


@anvil.server.callable
def next_nemo(n=5):
    res = execute(
         'select toponym_id, name from toponym where position_fk is  null order by source_fk, name')
    next_id, next_name = res[n]

    return next_id, next_name


@anvil.server.callable
def goto_disambiguator(n=5, nemo=False):
    """Go to position n of the toponyns with multiple suggestions

    Takes:
        n (int) - the position in the list to go to, there are checks to make
            sure that it is not less than 0 or greater than the length of the
            list of relevant toponyms.

    Returns:
        A  list of dictionaries with the relevant data for each suggestion for
        manual disambiguation
    """
    if n is None:
        n = 0

    if nemo:
        suggestion = 'nemo'
        res = execute(
            'select toponym_id from toponym where position_fk is  null order by source_fk, name')

    else:
        suggestion = 'suggestion'

        res = execute(f'select added_toponym_fk from {suggestion} '
                  'join toponym on added_toponym_fk == toponym_id '
                  'where outcome is NULL and added_toponym_fk not in '
                  f'(select added_toponym_fk from {suggestion} where outcome '
                  '== TRUE) and added_toponym_fk in (select toponym_id '
                  'from toponym where position_fk is NULL) '
                  'group by added_toponym_fk '
                  'order by toponym.source_fk, toponym.name ')

    N = len(res)

    if N == 0:
        return (0, 0), [{'added_toponym_id': 'No toponyms', 'newname': 'to',
                         'source_fk': 'disambiguate', 'oldid': 'N/A',
                         'oldname': 'N/A', 'suggestioncomment': 'N/A',
                         'pid': 'N/A', 'pcomment': 'N/A', 'paltnames': 'N/A',
                         'adminname': 'N/A'}]
    elif n >= N:
        # Brining n into range
        n = N - 1
    elif n < 0:
        # Brining negative N into range
        n = max(N+n, 0)
    target_id = res[n][0]

    if nemo:
        make_nemo(target_id)

    values = {'target_id': target_id}

    query = 'select added_toponym_fk, nt.name, nt.source_fk, '\
            'stable_toponym_fk, ot.name, '\
            f'{suggestion}.comment, p.position_id, '\
            'p.comment, (select group_concat (name, " \n") from toponym '\
            'where position_fk == p.position_id), '\
            '(select name from parent_region where '\
            '  parent_id == p.parent_fk) as a, '\
            'p.latitude, p.longitude '\
            ' from toponym as nt '\
            'left outer join '\
            f'{suggestion} on added_toponym_fk == nt.toponym_id left outer join toponym '\
            'as ot on stable_toponym_fk == ot.toponym_id left outer join position as p '\
            'on ot.position_fk == p.position_id '\
            ' where outcome is NULL and '\
            'added_toponym_fk == :target_id order by p.comment, a asc '
    results = [{'added_toponym_id': added_toponym_id,
                'newname': newname,
                'source_fk': source_fk,
                'oldid': oldid,
                'oldname': oldname,
                'suggestioncomment': suggestioncomment,
                'pid': pid,
                'pcomment': pcomment,
                'paltnames': paltnames,
                'adminname': adminname,
                'latitude': latitude,
                'longitude': longitude,
                'selected': True} for added_toponym_id, newname, source_fk,
                                      oldid, oldname, suggestioncomment,
                                      pid, pcomment, paltnames,
                adminname, latitude, longitude in execute(query, values=values, status='Disambiguation')]

    # if len(results) == 0:
    #     results = [{'added_toponym_id': target_id,
    #             'newname': 'No',
    #             'source_fk': 'Results',
    #             'oldid': 'Found',
    #             'oldname': '-',
    #             'suggestioncomment': 'Rerun',
    #             'pid': None,
    #             'pcomment': 'auto',
    #             'paltnames': 'matcher',
    #             'adminname': '#',
    #             'latitude': 0,
    #             'longitude': 0,
    #             'selected': True}]

    return (n, N), results
## added the 'selected': True so that the checkboxes defaults to being selected.

@anvil.server.callable
def remove_disambiguation_options(target_id, option_ids, nemo=False):
    """Suggestions are rejected, to ensure that they are not suggested again"""
    print(target_id, option_ids)
    remove_query = 'update suggestion set outcome = FALSE where '\
                   'added_toponym_fk == ? '\
                   'and stable_toponym_fk == ?'
    if nemo:
        remove_query = remove_query.replace('suggestion', 'nemo')
    values = [(target_id, option) for option in option_ids]
    _ = execute(remove_query, values=values, many=True)


@anvil.server.callable
def delete_toponym(toponym_id):
    execute('delete from toponym where toponym_id == :toponym_id',
            values={'toponym_id': toponym_id}, status='Deleting toponym')


@anvil.server.callable
def delete_position(position_id):
    """Removing a position, and all its seeded toponyms"""
    values = {'position_id': position_id}
    execute('update toponym set position_fk = NULL where length(source_fk) > 6'
            ' and position_fk == :position_id', values=values,
            status='Nulling added toponym position')

    execute('delete from toponym where position_fk == :position_id',
            values=values, status='Removing seeded connected toponyms.')

    execute('delete from position where position_id == :position_id',
            status='Deleting position.', values=values)


@anvil.server.callable
def disconnect_position(toponym_id, position_fk):
    """Disconnect a position from a toponym

    Removes all suggestions and pairing between the toponym and position,
    and then records it as a rejected suggestion. Thereby ensuring that the
    pairing will not be suggested in the future.
    """

    #first we delete the suggestions connecting toponym to position
    execute('delete from suggestion where added_toponym_fk == :toponym_id '
            'and stable_toponym_fk in (select toponym_id from toponym where '
            'position_fk == :position_fk)',
            values={'toponym_id': toponym_id, 'position_fk': position_fk})

    #disconnect position
    execute('update toponym set position_fk = NULL where toponym_id == '
            ':toponym_id', values={'toponym_id': toponym_id})

    # fetching all the toponym connected to position
    toponyms = execute('select toponym_id from toponym where position_fk == '
                       ':position_fk', values={'position_fk': position_fk})

    toponyms = [(f'disconnected: {position_fk}', toponym_id, reject_id[0]) for
                reject_id in toponyms]
    if len(toponyms) > 0:

        execute('insert into suggestion (outcome, comment, '
                'added_toponym_fk, stable_toponym_fk) values (False, ?, ?, ?)',
                values=toponyms, status='Disconnecting position', many=True)

    return len(toponyms)


@anvil.server.callable
def fetch_position_toponyms(position_id):
    """Fetching the toponyms, with source_fk and id, from connected toponyms"""
    res = execute('select toponym_id, name, source_fk from toponym where '
                  'position_fk == :position_fk',
                  values={'position_fk': position_id},
                  status='Fetching position toponyms')

    res = '- ' + '\n- '.join([' : '.join((str(_) for _ in r)) for r in res])

    return res


@anvil.server.callable
def fetch_positions_with_names(positions):
    """
    # later: Position name fetcher is ugly.
    Takes an iterable of positions_ids,
    returns these positions' ids, coordinates and all the various names connected to it. ... do not think I need parent data for this. We will see.
    """
    str_positions = str(tuple(positions)).replace("'", '"')

    query = 'select position_id, name, longitude, latitude, '\
            'p.source_fk, parent_fk '\
            'from position as p '\
            'join toponym on position_id == position_fk where position_id in '\
            f'{str_positions} group by position_id, name order by '\
            'count(position_id), parent_fk desc'

    results = execute(query, status='Positions_for_point')

    if results is None or len(results) == 0:
        return ('No points were found!')
    results_dict = {}
    for position, name, longitude, latitude, source, parent in results:
        if position in results_dict:
            results_dict[position]['names'] += f'\n{name}'
        else:
            results_dict[position] = {'position': position,
                                      'names': name,
                                      'longitude': longitude,
                                      'latitude': latitude,
                                      'source': source,
                                      'parent': parent,
                                      'selected': True}
    print(results_dict)
    return [_ for _ in results_dict.values()]


@anvil.server.callable
def fetch_created_positions():
    """Fetches all the positions recorded manually in the app"""
    query = 'select toponym_id, name, position_id, '\
            'longitude, latitude from position join toponym '\
            'on position_id == position_fk where position_id like "M_%" '\
            'group by position_id'
    res = execute(query, status='Created position Names')
    return [{'toponym_id': t_id, 'name': name, 'position_id': p_id,
             'longitude': lng,
             'latitude': lat} for t_id, name, p_id, lng, lat in res]


@anvil.server.callable
def merge_positions(positions, longitudes, latitudes, new_name,
                    parent_ids, sources):
    new_longitude = mean(longitudes)
    new_latitude = mean(latitudes)

    def singularise(iterable):
        """Returns the most common value from an iterable"""
        if set(iterable) == 1:
            return iterable[0]
        else:
            c = Counter(iterable)
            return c.most_common(1)[0][0]

    parent_id = singularise(parent_ids)
    source = singularise(sources)

    # creating new point
    new_name = 'M_' + anyascii(new_name) + '_'

    # A quick and dirty way to make sure that the new ID is not taken
    existing_pos_id = execute('select position_id from position')
    o = 0
    while (new_name + str(o), ) in existing_pos_id:
        o += 1
    new_name += str(o)
    add_position(position_id=new_name, longitude=new_longitude,
                 latitude=new_latitude,
                 source=source, parent_id=parent_id,
                 comment=f'Merged from {positions=} - {sources=}')

    replace_old_query = 'update toponym set position_fk = ?, '\
                        'comment = comment || "position_merged" '\
                        'where position_fk == ?'
    values = [(new_name, position_id) for position_id in positions]
    execute(replace_old_query, values=values, many=True)

    # recording the change in old positions, to explain why they are no longer in use.

    mark_old_positions = 'update position set comment = ? || comment '\
                         'where position_id == ?'
    values = [(f'Merged into {new_name} \n', pid) for pid in positions]
    execute(mark_old_positions, values=values, many=True,
            status='marking old positions')

    # Resolve any solvable doubles:
    # Use position_ids to fetch name ids, to see if they have more than one candidate pos
    # but is it better to resolve this with a single complicated nested query or multiple queries?
    # Since they also have to be removed from the duplicates ... maybe twice.
    # Or just leave them in the duplicates and sanitise it later?



@anvil.server.callable
def disambiguate(target, option, nemo=False):
    """Disambiguates a toponym

    Takes:
        target - the toponym id to retrieve the position
        option - the toponym id to donate the position

    Records the position from the accepted option in the target toponyms row.
    Which is also added to the comment of the target topony.
    The suggested pairing is recorded as accepted in the  suggestion table.

    """

    values = {'option': int(option), 'target': target}

    print(target, option, values)

    suggestion = 'nemo' if nemo else 'suggestion'

    log_position = 'update toponym set position_fk = '\
                   '(select position_fk from toponym where toponym_id == '\
                   ':option), comment = "Disambiguated to " || :option || '\
                   '"\n" || comment where toponym_id == :target'
    execute(log_position, values=values)

    # for some reason this part does not get executed through the GUI.
    log_acceptance = f'update {suggestion} set outcome = TRUE '\
                     'where added_toponym_fk == :target '\
                     'and stable_toponym_fk == :option'

    execute(log_acceptance, values=values)


@anvil.server.callable
def fetch_languages():
    """Fetches the used languages from the settings file"""
    return settings.languages


def export_formatter(cell):
    """Format cells to str, and remove breaklines for tsv export"""
    try:
        float(cell)
        return str(cell)
    except ValueError:
        cell = cell.strip().replace('\n', '_')
        return f'"{cell}"'


@anvil.server.callable
def export_selection(source, no_source=None, source2=None):
    """Exports position data from source, and source2 but not in no_source"""
    query = 'select (select name from toponym where '\
             'position_fk = position_id order by toponym_id limit 1), '\
             'toponym.name, '\
             'toponym.source_fk, '\
             'position_id, '\
             'longitude, latitude, '\
             'year, '\
             'toponym_edited, toponym.comment '\
             'from toponym join position on position_id == position_fk '\
             'join source on source.name == toponym.source_fk '\
             f'where toponym.comment not like "%Declare%" '

    # fillin in the 'where' depending on the supplied source.
    if source == 'Use':
        query += 'and length(toponym.source_fk) > 6 '
    elif type(source) is not str or len(source) < 7 or len(source) > 10:
        return 'invalid source supplied.'
    else:
        query += ' toponym.source_fk == :source '
        if source2 is not None:
            query += ' and position_id in (select position_fk from toponym '\
                     'where source_fk == :source2) '

    if no_source is not None:
        query += 'and position_fk not in (select position_fk from toponym '\
                 'where source_fk == :no_source and position_fk is not NULL) '

    query += 'group by position_id'

    result = execute(query, values={'source': source, 'no_source': no_source,
                                    'source2': source2})

    if len(result) == 0:
        return (f'No results were found for {source}:{no_source}:{source2}')

    header = ['Toponym_first_used', 'Toponym_added', 'Source',
              'PositionID', 'Longitude', 'Latitude',
              'Year', 'Toponym_edited', 'Comment', ]

    return to_tsv(header, result)


@anvil.server.callable
def export_selection_by_year(source, no_source=None, source2=None):
    """Exports positions with year source, and source2 but not in no_source"""

    query = 'select (select name from toponym where '\
            'position_fk = position_id order by toponym_id limit 1), '\
            'toponym.name, '\
            'source.year, '\
            'position_id, '\
            'longitude, latitude, '\
            'year, '\
            'toponym_edited, toponym.comment '\
            'from toponym join position on position_id == position_fk '\
            'join source on source.name == toponym.source_fk '\
            'where '

    # fillin in the 'where' depending on the supplied source.
    if source == 'Use':
        query += 'length(toponym.source_fk) > 6 '
    elif type(source) is not int and len(source) != 4 and not source.isdigit():
        return 'invalid source supplied.'
    else:
        source = int(source)

        query += ' year == :source '
        if source2 is not None:
            source2 = int(source2)
            query += ' and position_id in (select position_fk from toponym '\
                     'join source on source_fk == source.name '\
                     'where year == :source2) '

    if no_source is not None:
        no_source = int(no_source)
        query += 'and position_fk not in (select position_fk from toponym '\
                 'join source on source_fk == source.name '\
                 'where year == :no_source and position_fk is not NULL) '

    query += 'group by position_id'

    result = execute(query, values={'source': source, 'no_source': no_source,
                                    'source2': source2})

    if len(result) == 0:
        return (f'No results were found for {source}:{no_source}:{source2}')

    header = ['Toponym_first_used', 'Toponym_added', 'Source',
               'PositionID', 'Longitude', 'Latitude',
               'Year', 'Toponym_edited', 'Comment', ]

    return to_tsv(header, result)


def to_tsv(header, results):
    """Joing headers with result rows in a tsv format"""
    tsv = ''
    for row in [header] + results:
        row = '\t'.join(export_formatter(cell) for cell in row)
        tsv += row + '\n'

    return tsv.strip()


@anvil.server.callable
def erase_positions(positions):
    """Removes a set of positions from the database

    Takes:
        positions - iterable containing position_ids


    The manually added toponyms are first disconnected from the positions, and
    the comment is updated with a message to reflect this action.
    Then the toponyms still connected to it (from seeding the database) are
    then removed.
    Next each suggestion linking to the positions are removed.
    Finally, the positions themselves are removed.
    """

    for position in set(positions):
        # reset the added, hence long source name, toponyms linked to it
        execute('UPDATE toponym SET position_fk=NULL, comment=:message '
                '|| "\n" || comment where position_fk==:position and '
                'length(source_fk) > 6 ',
                values={'position': position,
                        'message': f'Position: {position} was removed..'})

        # remove current suggestions to position
        execute('DELETE FROM suggestion where stable_toponym_fk in '
                '(select toponym_id from toponym where position_fk == '
                ':position)', values={'position': position})

        # remove remaining toponyms
        execute('DELETE FROM toponym WHERE position_fk == :position',
                values={'position': position})

        # remove position
        execute('DELETE FROM position where position_id == :position',
                values={'position': position})


@anvil.server.callable
def make_position_for_toponym(toponym_id, latitude, longitude, source):
    """Record a new position for a particular toponym

    Takes:
        toponym_id - the id of a toponym
        latitude, longitude - the coodtinate of the new position
        source - the source of this new position - typically the same as the
            toponym as it was probably approximated by studying the book.

    The position_id is created by concatenating "M_" with the toponym_id, so
    first it checks that there is no such position.

    Then the position is recorded with the supplied coordinates and a comment
    that it has been created manually.

    Finally the toponym is connected to the newly created position, with a
    comment that the position was created for the toponym.
    """

    position_id = f'M_{toponym_id}'

    id_used_count = execute('select count(position_id) from position where '
                            'position_id == :position_id',
                            values={'position_id': position_id})[0][0]

    if id_used_count > 0:
        logging.critical(f'{position_id=} is already registered.')
        raise ValueError('position_id already exists')

    add_position(position_id, source=source, latitude=latitude,
                 longitude=longitude, parent_id='Manual',
                 comment='Created manually')

    # connect the toponym
    connect_toponym(toponym_id=toponym_id,
                    position_fk=position_id,
                    comment=f'Created position {position_id} for toponym')


@anvil.server.callable
def rename_toponym(toponym_id, toponym):
    """Changes the recorded name and derivatives of a toponym"""
    tokens, asciiname, asciitokens, pattern = preprocess_toponym(toponym)

    execute('UPDATE toponym SET name = :toponym, comment = :message || '
            'name ||  "\n" || comment , tokens = :tokens, '
            'asciiname = :asciiname, asciitokens = :asciitokens, '
            'pattern = :pattern where toponym_id == :toponym_id and '
            'name != :toponym',
            values={'toponym': toponym.strip(), 'toponym_id': toponym_id,
                    'message': 'Updated manually from ',
                    'tokens': tokens,
                    'asciiname': asciiname,
                    'asciitokens': asciitokens,
                    'pattern': pattern})


@anvil.server.callable
def connect_created_position(toponym_id, position_fk, comment):
    """Connects a (created) position to the toponym"""
    connect_toponym(toponym_id=toponym_id, position_fk=position_fk,
                    comment=comment)


@anvil.server.callable
def cluster(sources, radius):
    """Clusters points based on distance and returns it in TSV format
    Takes:
        sources - a list of source.name to be included in the clustering
                    process and export.
        radius (float) - the radius (in km) to be used for clustering

    After recalculating the radius to decimal hours, each pair of points witih
    one radius of each other is placed in the same cluster.
    Ergo, the distance between

    """

    # parse sources for the query
    # source_grp = '(' + ', '.join(f'"{s}"' for s in sources) + ')'
    source_grp = ' OR '.join(f' source_fk == "{s}" ' for s in sources)
    source_grp = f' ({source_grp}) '

    source_cnt = ', '.join(f'sum(source_fk == "{s}") ' for s in sources)

    # erase table, recreate
    execute('DROP TABLE IF EXISTS cluster', status='Erasing cluster table')
    execute('CREATE TABLE cluster (p_fk, lat, lng, cluster_nr)')

    # calculate the new cluster
    p_query = 'select position_id, latitude, longitude from position where '\
              'position_id in (select position_fk from toponym where '\
              ' ' + source_grp + ' '\
              'and comment not like "%Declared%" group by position_fk)'

    c_query = 'select cluster_nr, lat, lng from cluster where '\
              'lat between :lat_lo and :lat_hi '\
              'and lng between :lng_lo and :lng_hi '\
              'order by cluster_nr'

    # In the worst case: At the equator each degree is a 111km radius,
    equator_radius = radius/111

    for position_id, latitude, longitude in execute(p_query,
                                                    status='P for Clustering'):
        # for some reason including this in the query grinds it to a halt.
        if position_id == 0:
            continue

        clusters = []
        lat_lo = latitude - equator_radius
        lat_hi = latitude + equator_radius
        lng_lo = longitude - equator_radius
        lng_hi = longitude + equator_radius
        for c, c_lat, c_lng in execute(c_query,
                                       values={'lat_lo': lat_lo,
                                               'lat_hi': lat_hi,
                                               'lng_lo': lng_lo,
                                               'lng_hi': lng_hi}):
            if c in clusters:
                continue
            elif position_distance((latitude, longitude),
                                   (c_lat, c_lng)) <= radius:
                clusters.append(c)

        # search completed
        if len(clusters) == 0:
            c = execute('select max(cluster_nr) from cluster')[0][0]
            if c is None:
                c = 0
            else:
                c += 1
        else:
            c = clusters.pop(0)
            while len(clusters) > 0:
                c2 = clusters.pop()
                execute('update cluster set cluster_nr == :c where '
                        'cluster_nr == :c2',
                        values={'c': c, 'c2': c2})
        # insert the new position
        execute('insert into cluster (p_fk, lat, lng, cluster_nr) values '
                '(:p_fk, :lat, :lng, :cluster)',
                values={'p_fk': position_id,
                        'lat': latitude,
                        'lng': longitude,
                        'cluster': c})

    # query, format and return.c
    e_query = 'select group_concat(distinct year) as s, avg(lat), '\
              'avg(lng), count(distinct p_fk), count(source_fk), '
    e_query += source_cnt
    e_query += ' from toponym as t join cluster on position_fk == p_fk '\
               'join source on t.source_fk == source.name '\
               'where ' + source_grp + ' group by cluster_nr '\
               'order by s'

    result = execute(e_query, status='Cluster-export')
    if len(result) == 0:
        return 'No results found'

    header = ['sources', 'Latitude', 'Longitude', 'cnt_points', 'cnt_sources']
    header += [f'cnt_{source}' for source in sources]

    return to_tsv(header, result)


if __name__ == '__main__':
    logging.basicConfig(
        filename='toponym.log',
        level=logging.DEBUG,
        format='%(asctime)s %(message)s',
        datefmt='%Y-%m-%d @ %H:%M:%S ',
        force=True
        )

    create_tables()

    # Starting server with settings.server_token
    #
    anvil.server.connect(settings.server_token)

    make_nemo_list()

    anvil.server.wait_forever()
