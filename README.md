[![Python 3.9](https://img.shields.io/badge/python-3.9-blue.svg)](https://www.python.org/downloads/release/python-395/)
[![SQLite3](https://img.shields.io/badge/SQLite-07405E?style=for-the-badge&logo=sqlite&logoColor=white)](https://www.sqlite.org/index.html)
[![AnvilWorks](https://anvil.works/ide/img/logo-35.png)](https://anvil.works/)

# CITADEL[^1]


The accompanying code is the backend of a toponym disambiguation tool
developed and employed for the purpose of the following aricle:

<!-- LATER : Add citation to paper here -->

It has been assembled by a range of open-source tools, so anyone interested
can simply clone this repository and apply it to their own needs.
If you do, we kindly ask that you cite the abovementioned article/paper and
this repository.

In tandem with this backen we have also constructed a graphical user interface
through the [AnvilWorks](https://Anvil.Works) application framework.

## Design Rationale

At the core of the application is a small relational database powered but
![SQLite](http://sqlite.org)
which we fill with known positions from the countries we are interested in and
all the toponyms that we can find in the relevant languages. This gives the
application something to compare added, unknown toponyms to with the hope of
finding their correct location.

After adding new toponyms to the database each toponym is compared agains all
toponyms that has a link to a position in a series of sequantially looser
string comparisons, if it finds but a single matching position this is recorded
and when there are multiple options all of these suggestions are recorded and will need to be manually disambiguaged using the GUI.

### Application Part 1 - Python

The key part of the application lies in the python code which is run either
locally on a machine or on a server somewhere. This backend has been written
for and tested in Python 3.9.5,
<!-- Todo: Check all python versions -->
and should therefore work for all Python 3.9.5+ versions.

### Application Part 2 - Anvilworks

The graphical user interface is created using
[!AnvilWorks](https://anvil.works), and all you need
in order to run this application is a free-tier account.


## Setup

1. Install the requirements from the requirements.txt file.

2. Run
```settings.py ```
to select which countries and languages should be used to seed the database.
For a complete set of instructions of how the script works run
```settings.py -h```

3. Run
```seed.py```
to fill the database with positions and toponyms from
![GeoNames](https://GeoNames.org) and further alternative names from
![WikiData](https://WikiData.org) from all the selected countries and in
all the selected languages.

    - Retreiving data from WikiData can take a long time, and if the script gets interrupted during this stage of the process run ```operations.py``` to finish retreiving data from WikiData.

4. https://anvil.works/build#page:apps - [import from file]
Take note of your server token and use it to replace the placeholder in your
settings.yaml file. This token is personal and should not be shared with anyone
it will let the webb interface interact with your local python installation.

## Using the application

In order to start the server all you need to do is to run the
```toponym_main.py``` script and it will then be ready to take instructions
from the webb application and handle all connections to the database.

### Adding toponyms

todo: add text

### Running the auto matcher

todo: add text

### Disambiguation

todo: add text

#### Weak mathing

todo: add text


# Exporting

There are two types of exports supported in the GUI, a .tsv file with the
toponyms and their associated positions by source or year and a clustered
export.

## Regular TSV

For these exports you select a source (year) to include, with the option of
including another source (year) and excluding another source (year).
The results will be presented in a simple text area that can be copied and
pasted into a .tsv file of your choice.

## Clustering - also TSV

The second export function gives an outpout similar to the primary export,
with one important different: It first clusters points that are within a set
radius.
Clustering may be necessary for two reasons, either there are many toponyms that
required a lot of unique coordinates to be recorded, splintering the toponyms
unecessarity. Or, there are too many toponyms in a relatively small area, which
is difficult to plot properly on a national level map.

For simplicity's sake the distance between two points is calculated using the
cartesian distance between coordinates (longitude and latitude).
Any two used positions (linked to an added toponym) within the set radius,
calculated as the cartesian distance between the coordinates, are put in the
same cluster.
This simple clustering appriach means that we have to be careful when selecting
the radius:
In the unlikely event that there is a long line of points within a short
distance from eachother this long line would become a single cluster.
Each cluster's final coordinates is calculated as the unweighted arithmetic
mean latitude and longitude of all points in the cluster.


# Todo

[] - Make the Browsing facility more user friendly

[] - Add user authentication and logging

[] - Add attractions


# footnotes

[^1]: pla**C**ename d**I**sambigua**T**ion **A**n**D** g**E**ocoding app**L**ication [![Acronymify](https://img.shields.io/badge/Acronymify-Citadel-lightgrey)](https://acronymify.com/CITADEL/?q=Placename+disambiguation+and+geocoding+application)

