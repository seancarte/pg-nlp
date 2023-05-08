#!/usr/bin/env python
'''
NAME: ir_nlp_analysis.py

SYNOPSIS: ./ir_nlp_anlaysis.pl <kwd_file_dir> [collection_uuid] [item_type] [details]

DESCRIPTION:
creates a docs table; populated with abstracts made available between 2012 and 2021, inclusive
optionally limited to a collection_uuid and item type

expects a directory populated with files: sdg01 ... sdgn

provides a list of each keyword in the kwd_file that occurs in an abstract, with the source
foreach keyword/phrase, count occurences in docs
"$sdg|$phrase|$kwds{$sdg}{$phrase}|source"

requires python3-dotenv, python3-psycopg2, argparse

requires a .env file:
DEBUG=true
USERNAME=dspace
PASSWORD=_password_
HOST=127.0.0.1
PORT=5432
DBNAME=dspace

DATE: 2023-05-04
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
import argparse

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
kwd_matches = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

# @ARGV -------------------------------------------#
parser = argparse.ArgumentParser()
parser.add_argument('-c', '--collection_uuid', type=str, help="collection uuid")
parser.add_argument('-d', '--dir', type=str, help="directory containing the kwd files")
parser.add_argument('-e', '--details', type=str, help="print details")
parser.add_argument('-f', '--from_date', type=str, help="date from")
parser.add_argument('-t', '--to_date', type=str, help="date to")
parser.add_argument('-i', '--item_type', type=str, help="item type: Article or Thesis")
args = parser.parse_args()
if args.dir is None:
    print("you need to enter a directory -d")
    sys.exit()
else:
    input_dir = args.dir

input_uuid = ''
if args.collection_uuid is not None:
    if not re.search(r'^[\w\-]+$', args.collection_uuid):
        print("the collection uuid -c must be a uuid")
        print('Usage: %s -d directory [-c collection_uuid -e details -i item_type -f from_date -t to_date]' % sys.argv[0])
        sys.exit()
    input_uuid = args.collection_uuid

input_from_date = 0
input_to_date = 0

if args.from_date is not None:
    if not re.search(r'^\d{4}', args.from_date):
        print("the from_date -f must be at least a year: YYYY")
        print('Usage: %s -d directory [-c collection_uuid -e details -i item_type -f from_date -t to_date]' % sys.argv[0])
        sys.exit()
    if (args.to_date is None):
        print("you need to enter a to_date -t: YYYY")
        print('Usage: %s -d directory [-c collection_uuid -e details -i item_type -f from_date -t to_date]' % sys.argv[0])
        sys.exit()
    if not re.search(r'^\d{4}', args.to_date):
        print("the to_date -f must be at least a year: YYYY")
        print('Usage: %s -d directory [-c collection_uuid -e details -i item_type -f from_date -t to_date]' % sys.argv[0])
        sys.exit()
    input_from_date = args.from_date
    input_to_date = args.to_date

input_type = ''
if args.item_type is not None:
    args.item_type.strip()
    if not (args.item_type == 'Article' or args.item_type == 'Thesis'):
        print('the type must be "Article" or "Thesis"')
        print('Usage: %s -d directory [-c collection_uuid -e details -i item_type -f from_date -t to_date]' % sys.argv[0])
        sys.exit()
    input_type = args.item_type

print_details = ''
if args.details is not None:
    if not args.details == 'details':
        print('Usage: %s -d directory [-c collection_uuid -e details -i item_type -f from_date -t to_date]' % sys.argv[0])
        sys.exit()
    print_details = args.details

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

# keywords
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

# corpus
sql = 'DROP TABLE docs;'
try:
    cur.execute(sql)
    # result = cur.fetchone()[0]
    conn.commit()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
    sys.exit()

sql = 'CREATE TABLE docs (id SERIAL, resource_id TEXT, title TEXT, abstract TEXT, PRIMARY KEY(id));'
try:
    cur.execute(sql)
    # result = cur.fetchone()[0]
    conn.commit()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
    sys.exit()

sql = "CREATE INDEX gin2 ON docs USING gin(to_tsvector('english', abstract));"
try:
    cur.execute(sql)
    # result = cur.fetchone()[0]
    conn.commit()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
    sys.exit()

sql = """INSERT INTO docs(resource_id, title, abstract)
SELECT DISTINCT title.dspace_object_id, title.text_value as title, abstract.text_value as abstract
FROM item i
JOIN metadatavalue abstract
ON abstract.dspace_object_id = i.uuid
JOIN metadatavalue date
ON date.dspace_object_id = i.uuid
JOIN metadatavalue title
ON title.dspace_object_id = i.uuid
JOIN metadatavalue type
ON type.dspace_object_id = i.uuid
WHERE abstract.metadata_field_id = 27
AND title.metadata_field_id = 64 AND title.text_value NOT LIKE '%.___' AND title.text_value NOT LIKE '%\\_____' AND title.text_value != 'TEXT' AND title.text_value != 'LICENSE' AND title.text_value != 'ORIGINAL' AND title.text_value != 'THUMBNAIL' AND title.text_value != 'TEXT'
AND i.in_archive = 't' AND i.withdrawn = 'f' AND i.discoverable = 't'"""

if input_uuid:
    sql += " AND i.owning_collection = '""" + input_uuid + "'"

if input_from_date and input_to_date:
    sql += " AND date.metadata_field_id = 15 AND date.text_value >= '" + input_from_date + "' AND date.text_value < '" + input_to_date + "'"
else:
    sql += " AND date.metadata_field_id = 15 AND date.text_value >= '2012' AND date.text_value < '2022'"

if input_type:
    sql += " AND type.metadata_field_id = 66 AND type.text_value = '" + input_type + "'"

try:
    cur.execute(sql)
    conn.commit()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
    sys.exit()


sql = 'SELECT COUNT(*) FROM docs;'
try:
    cur.execute(sql, (input_uuid,))
    result = cur.fetchone()[0]
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
    sys.exit()

print('number of items in docs: ', result)

# create tsqueries
# weed out duplicate requests
# ## websearch_to_tsquery requires postgres > 9.5
terms = {}
sql = "SELECT websearch_to_tsquery('english', %s);"
for sdg in kwds:
    # print(sdg)
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
sql = "SELECT resource_id FROM docs WHERE to_tsquery('english', %s) @@ to_tsvector('english', abstract);"

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
                kwd_matches[sdg][resource_id]['count'] += 1
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
# pp.pprint(kwd_matches)
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

for sdg in sorted(kwd_matches):
    print('%s: %d' % (sdg, len(kwd_matches[sdg])))
