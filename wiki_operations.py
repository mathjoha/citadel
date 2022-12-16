# coding=<utf-8>
import requests
import os
import logging
from qwikidata.sparql import return_sparql_query_results as sparql
from settings import settings
from tqdm import tqdm
from collections import defaultdict

from string import Template
query_template = Template("SELECT ?item ?language"
                          "(group_concat(?label;separator='___')as ?toponym) "
                          "where "
                          "{ VALUES ?item { $items } VALUES ?language { "
                          "$languages } ?item (rdfs:label|skos:altLabel) "
                          "?label"
                          " filter(lang(?label)=?language) } "
                          "group by ?item ?language")

wiki_title_2_base_item = 'wiki_title_2_base_item.txt'

if not os.path.exists(wiki_title_2_base_item):
    with open(wiki_title_2_base_item, 'w') as f:
        pass

wiki_dict_dir = 'wiki_dictionaries'

os.makedirs(wiki_dict_dir, exist_ok=True)


def extract_base_item(response_dict):
    """Recursive function to find the 'wikibase_item' from nested dict"""
    for key, item in response_dict.items():
        if key == 'wikibase_item':
            return item
        elif type(item) == dict:
            item2 = extract_base_item(item)
            return item2


def fetch_base_item(link):
    """Takes a wikipedia link and puts the wikidata identifier in a txt file"""
    if 'wikipedia.org' not in link:
        return None

    name = link.split('/')[-1]
    lang = link.split('.')[0][-2:]

    # later: simple but inefficient. -- but I cannot be sure that titles make acceptable filepaths  # noqa: E501
    with open(wiki_title_2_base_item, 'r', encoding='utf8') as f:
        for line in f.readlines():
            if line != '':
                title, base_item = line.split('___')
                if title == name:
                    return base_item.strip()

    url = f'https://{lang}.wikipedia.org/w/api.php?action=query&prop='\
          f'pageprops&format=json&titles={name}'
    logging.debug(f'Could not find {name} in record, downloading.')

    response = requests.get(url)

    if response.status_code != 200:
        raise requests.HTTPError(f'{url=} led to {response.status_code=}')

    result_dict = response.json()

    base_item = extract_base_item(result_dict)

    # if base_item is None or not base_item.startswith('Q'):
    #     logging.debug(f'{link=} has not additional data.')
    #     return None

    with open(wiki_title_2_base_item, 'a', encoding='utf8') as f:
        f.write(f'{name}___{base_item}\n')

    return base_item


def get_wiki_names(identifiers, languages=settings.languages):
    """Queries WikiData for all names in a set of languages for the IDs"""
    items = ' '.join(f'wd:{q.upper()}' for q in identifiers)
    languages = ' '.join(f"'{lang}'" for lang in languages)

    query = query_template.substitute(items=items, languages=languages)

    result_dict = defaultdict(list)
    for row in tqdm(sparql(query)['results']['bindings'],
                    desc='Parsing SPARQL query result.'):
        identifier = row['item']['value'].split('/')[-1]
        toponyms = row['toponym']['value']
        language = row['language']['value']
        result_dict[identifier.lower()].append((toponyms, language.lower()))
    return result_dict
