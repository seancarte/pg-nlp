#!/usr/bin/env python
'''
NAME: ir_scival_corpus_sdg_analysis.py

SYNOPSIS: ./ir_scival_corpus_sdg_analysis.pl <sdg_file_dir> [sdg] [details]

DESCRIPTION:
provides a list of each keyword in the sdg_file that occurs in an abstract, with the source
foreach keyword/phrase, count occurences in docs
"$sdg|$phrase|$kwds{$sdg}{$phrase}|source"

requires python3-dotenv, python3-psycopg2

requires a .env file:
DEBUG=true
USERNAME=dspace
PASSWORD=_password_
HOST=127.0.0.1
PORT=5432
DBNAME=dspace

DATE: 2023-01-16
'''

# Import -----------------------------------------#
import os
from dotenv import load_dotenv
import sys
import os.path
import pathlib
import re
# import csv
import pprint
# import datetime
import psycopg2
from collections import defaultdict

# Define ------------------------------------------#
load_dotenv()  # these constants live in .env

DEBUG = os.getenv('DEBUG')
DBNAME = os.getenv('DBNAME')
PORT = os.getenv('PORT')
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')
HOST = os.getenv('HOST')

# DEBUG = 0

# count of resource_ids matched for each sdg: sdg[sdg][resource_id]['count']
sdg_matches = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

# @ARGV -------------------------------------------#
if len(sys.argv) < 2:  # no parameters
    print('Usage: %s directory [sdg] [details]' % sys.argv[0])
    sys.exit()

input_dir = sys.argv[1]
if not os.path.exists(input_dir):
    print('Dir not found: %s' % input_dir)
    sys.exit()

# optional sdg
input_sdg = 0
if len(sys.argv) > 2:
    input_sdg = sys.argv[2]
    if not re.search(r'^\d+$', input_sdg):
        print('second parameter must be a digit')
        sys.exit()

# optional print details
print_details = 0
if len(sys.argv) > 3:
    print_details = sys.argv[3]
    if not print_details == 'details':
        print('Usage: %s directory [sdg] [details]' % sys.argv[0])
        sys.exit()

# Initialise -------------------------------------- #
pp = pprint.PrettyPrinter(indent=4)

# -------------------------------------------------- #

# connect to db
try:
    conn = None
    conn = psycopg2.connect(
            database=DBNAME,
            user=USERNAME,
            password=PASSWORD,
            host=HOST,
            port=PORT
    )
    conn.autocommit = False
    cur = conn.cursor()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
    sys.exit()

# SDG keywords
files = {}
d = pathlib.Path(input_dir)
for entry in d.iterdir():
    if entry.is_file():
        f = str(entry)
        try:
            res = re.search(r'sdg(\d+)$', f)
            sdg = res.group(1)
            files[sdg] = f
        except Exception as e:
            print(e)
            print('no sdg file found found for: %s' % f)
            continue

kwds = defaultdict(lambda: defaultdict(int))
for sdg in files:
    file = files[sdg]
    kwds[sdg] = {}
    with open(file) as f:
        for line in f:
            line = line.strip()
            kwds[sdg][line] = 0

# create tsqueries
# weed out duplicate requests
# ## websearch_to_tsquery requires postgres > 9.5
terms = {}
sql = "SELECT websearch_to_tsquery('english', %s);"
for sdg in kwds:
    # print(sdg)
    if input_sdg:  # only consider input sdg
        if sdg != input_sdg:
            continue
    terms[sdg] = {}
    for phrase in kwds[sdg]:
        try:
            # print(phrase)
            cur.execute(sql, (phrase,))
            result = cur.fetchall()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            sys.exit()
        # finally:
        #    if conn is not None:
        #        conn.close()
        if result:
            for line in result:
                term = line[0].strip('\"')
                term = line[0].replace('\'', '')
                terms[sdg][term] = 0

# pp.pprint(terms)
# sys.exit()

# tsquery with cooked terms
# ids that match
items = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # information about each item
sql = "SELECT resource_id FROM docs WHERE to_tsquery('english', %s) @@ to_tsvector('english', doc);"
# sql = "SELECT resource_id FROM docs WHERE to_tsquery('english', %s) @@ to_tsvector('english', abstract);"

for sdg in terms:
    print('processing sdg %s ...' % sdg)
    terms[sdg]['total'] = 0
    for phrase in terms[sdg]:
        if phrase == 'total':
            continue
        if print_details:
            print(phrase)
        try:
            cur.execute(sql, (phrase,))
            result = cur.fetchall()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            sys.exit()
        if result:
            if DEBUG:
                print('phrase: ', phrase)
            for line in result:
                if DEBUG:
                    print('result: ', line)
                resource_id = line[0]
                sdg_matches[sdg][resource_id]['count'] += 1
                items[resource_id][sdg]['count'] += 1
                items[resource_id][sdg][phrase] += 1
                try:
                    terms[sdg][phrase]['count'] += 1
                except (Exception, KeyError):
                    terms[sdg][phrase] = 0
                    terms[sdg][phrase] += 1
                terms[sdg]['total'] += 1


# pp.pprint(items)
# pp.pprint(terms)
# pp.pprint(sdg_matches)
# sys.exit()

'''
# total matches for phrases
for sdg in terms:
    print('%s: %d' % (sdg, terms[sdg]['total']))

# total items that matched
for resource_id in items:
    for sdg in items[resource_id]:
        if DEBUG:
            print('%s: %d' % (resource_id, items[resource_id][sdg]['count']))
'''

for sdg in sorted(sdg_matches):
    print('%s: %d' % (sdg, len(sdg_matches[sdg])))
