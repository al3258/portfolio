"""
 homework_2.7.py -- etl library

 Author: Amanda Li (al3258@nyu.edu)
 Last Revised: 9/24/2023
"""

import csv
import io
import sqlite3
import json


def sql2csv(query, conn):
    """
    query a database and return string data in CSV format

    Arguments:  query   string SQL query to select data
                conn    database connection object

    Return Value:  a string with CSV-formatted data

    """

    conn = sqlite3.connect(conn)
    cursor = conn.cursor()
    cursor.execute(query)

    headers = [items[0] for items in cursor.description]

    writestring = io.StringIO()
    writer = csv.writer(writestring)
    writer.writerow(headers)

    for tup in cursor:
        writer.writerow(tup)

    conn.close()

    writestr = writestring.getvalue()
    writestring.close()

    return writestr


def sql2json(query, conn, format='lod', primary_key=None):
    """
    query a database and return a JSON string

    if format is 'lod', function will return a
    list of dicts

    (If format is 'lod' and primary_key is specified,
     function should raise ValueError with a suitable
     message.)

    if format is 'dod', function will return a dict
    of dicts with the designated primary_key as
    "outer" dict key

    (If format is 'dod' and primary_key is not specified,
     function should raise ValueError with a suitable
     message.)

    Arguments:  query   string SQL query to select data
                conn    database connection object

                format (optional):
                        'lod' (list of dicts, the default) or
                        'dod' (dict of dicts)

                primary_key  (optional):
                        column value to use as the key
                        for a dict of dicts

                (note that if format == 'dod' then 'primary_key'
                 must be an existing column name;
                 if format == 'lod' then 'primary_key'
                 must be None -- use 'is None' or 'is not None'
                 to test)

    Raises:  ValueError if format is 'dod' and primary_key is
             not specified, or format is 'lod' and primary_key
             is specified.

    Return Value:  string in JSON format

    """

    conn = sqlite3.connect(conn)
    cursor = conn.cursor()
    cursor.execute(query)

    headers = [items[0] for items in cursor.description]

    if format == 'dod' and primary_key not in headers:
        raise ValueError(f'an existing column must be provided as the primary_key if format is {format}')
    elif format == 'lod' and primary_key is not None:
        raise ValueError(f'primary_key must be None if format is {format}')

    if format == 'dod':
        dod = {}
    elif format == 'lod':
        lod = []

    for r in cursor:
        in_d = {}

        for col in r:
            in_d[headers[r.index(col)]] = r[r.index(col)]

        if format == 'dod':
            dod[r[headers.index(primary_key)]] = in_d
        elif format == 'lod':
            lod.append(in_d)


    if format == 'dod':
        return json.dumps(dod, indent=4)
    elif format == 'lod':
        return json.dumps(lod, indent=4)


def query_create(header_row, row, table):
    """
    Write a CREATE TABLE and INSERT INTO statements
    to load data into a database using sql

    Arguments:  header_row  headers
                row         values
                table       table to insert to

    Return Value:  CREATE TABLE statement (str), INSERT INTO statement (str)
    """

    collist = []
    for header in header_row:
        final_typname = None

        for typtype, typname in ((int, 'INT'), (float, 'REAL')):

            try:
                typtype(row[header_row.index(header)])
                final_typname = typname
                break

            except ValueError:
                pass

        if not final_typname:
            final_typname = 'TEXT'

        collist.append(f'{header} {final_typname}')

    colstr = ', '.join(collist)

    valstr = ', '.join(['?'] * len(header_row))

    create_table = f'CREATE TABLE {table} ({colstr})'
    insert_into = f'INSERT INTO {table} VALUES ({valstr})'

    return create_table, insert_into

def csv2sql(filename, conn, table):
    """
    insert a csv file into a database table

    Arguments:  filename   CSV filename to read
                conn       database connection object
                table      table to insert to

    Return Value:  None (writes to database)
    """

    readfile = open(filename)
    reader = csv.reader(readfile)
    readheader = next(reader)

    conn = sqlite3.connect(conn)
    c = conn.cursor()

    c.execute(f'DROP TABLE IF EXISTS {table}')

    query = None

    for r in reader:
        if not query:
            create, query = query_create(readheader, r, table)
            c.execute(create)
        c.execute(query, r)

    conn.commit()
    conn.close()


def json2sql(filename, conn, table):
    """
    insert JSON data into a database

    Arguments:  filename   JSON file to read (assumes dict of dicts)
                           also assumes that "inner" dicts all have
                           identical keys
                conn       database connection object
                table      name of table to write to

    Return Value:  None (writes to database)
    """
    file = open(filename)
    dod = json.load(file)
    conn = sqlite3.connect(conn)
    c = conn.cursor()

    c.execute(f'DROP TABLE IF EXISTS {table}')

    query = None

    for in_v in dod.values():
        if not query:
            create, query = query_create(list(in_v.keys()), list(in_v.values()), table)
            c.execute(create)
        c.execute(query, list(in_v.values()))

    conn.commit()
    conn.close()