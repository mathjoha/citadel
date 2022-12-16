# coding=<utf-8>

from settings import settings
import sqlite3


def create_tables():
    '''

    creates and configures the database at the intended location.

    '''
    conn = sqlite3.connect(settings.database_path)
    # 6 chars should be enough to create unique enough names for quick
    # AND intuitive identification of the various sources ...
    # there are not that many sources after all
    conn.execute('CREATE TABLE IF NOT EXISTS source '
                 '(name char(6) primary key, comment text,'
                 ' year INTEGER not NULL, source_date_edited)')
    # Using "comment" instead of "title" gives us the opportunity to cover more
    # than just the title, we can also write something or paset URL...
    conn.execute('CREATE TRIGGER IF  NOT EXISTS '
                 'set_source_edit AFTER INSERT ON source BEGIN '
                 'UPDATE source set source_date_edited = datetime("now") where'
                 ' name == new.name; end')
    # A trigger that updates the time to the last time any source was edited.
    # ... though is this a good design pattern?

    # Do we need a language table? we can still have it on GUI side w/o table

    # the toponym table contains all the available toponyms!
    conn.execute('CREATE TABLE IF NOT EXISTS toponym '
                 '(toponym_id integer primary key, '
                 'position_fk, '
                 'source_fk char(6) not null, '
                 'name not null, '
                 'asciiname not null, '
                 'pattern, '
                 'tokens, '
                 'language, '
                 'asciitokens, '
                 'comment text, '
                 'toponym_created_date datetime, '
                 'toponym_edited datetime '
                 ')')
    # position key will probably not be set, I will need int for GeoNames,
    # perhaps using the negative GeoNames ID for "main" name.
    # And I want to leave the possibility open for importing WikiData positions

    conn.execute('CREATE TRIGGER IF  NOT EXISTS '
                 'set_toponym_edit after update on toponym '
                 'begin update toponym set toponym_edited = datetime("now") '
                 'where toponym_id == old.toponym_id; end;')

    # position - pk is open for the same reson toponym key is
    conn.execute('CREATE TABLE IF NOT EXISTS position '
                 '(position_id primary key, '
                 'source_fk not NULL, latitude REAL not NULL, '
                 'longitude REAL not NULL, '
                 # 'abandoned bool, '
                 # 'parent_name not null, '
                 'parent_fk not null, '
                 'comment text, '
                 'position_created datetime not null, '
                 'position_edited datetime '
                 ')')
# TODO : Geonames : feature class and name.
# Parent name is taken either from GeoNames or WikiData, so their id fields type need to remain open ended.  # noqa: E501

    conn.execute('CREATE TRIGGER IF  NOT EXISTS '
                 'set_position_edit after update on position '
                 'begin update position set position_edited = datetime("now") '
                 'where position_id == old.position_id; end;')

    # The suggestions table
    conn.execute('CREATE TABLE IF NOT EXISTS suggestion '
                 '(added_toponym_fk, '
                 'stable_toponym_fk, comment text, outcome bool)')
    # keeping track of the outcome in a bool:
    # null means not investigated
    # true means accepted match,
    # FALSE means denied match -- keeping track of this prevents adding the
    #  same pairs again.
    conn.execute('CREATE TABLE IF NOT EXISTS nemo '
                 '(added_toponym_fk, '
                 'stable_toponym_fk, comment text, outcome bool)')

    # The admin regions table
    conn.execute('CREATE TABLE IF NOT EXISTS parent_region '
                 '(parent_id primary key, '
                 'name text)'
                 )

    # WikiData bookkeeping
    conn.execute('CREATE TABLE IF NOT EXISTS wiki_queue '
                 ''
                 '(wiki_id , position_fk, source_fk, title, processed)')

    conn.commit()
