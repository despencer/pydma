#!/usr/bin/env python

from datetime import datetime
import pyodbc
import json

def convert(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return value

def dumptable(cursor, table, target, name=None):
    if name == None:
        name = table
    result = []
    print(table)
    for dbrow in cursor.execute("select * from " + table):
        row = {}
        for c in cursor.description:
            row[c[0]] = convert(dbrow.__getattribute__(c[0]))
        result.append(row)
    with open(target + '/' + name + '.data', 'w') as tfile:
        json.dump({'table':table, 'data':result}, tfile, indent=4, ensure_ascii=False)

def main():
    import argparse
    import getpass

    parser = argparse.ArgumentParser(description="Dumps the structure and the data from a MSSQL database")
    parser.add_argument("database", type=str, help="Database name")
    parser.add_argument("target", type=str, help="Target directory")
    args = parser.parse_args()

    passwd = getpass.getpass('Password:')
    db = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost;DATABASE='+args.database+';TrustServerCertificate=yes;UID=sa;PWD='+passwd)
    db.setencoding(encoding='utf-8')
    cursor = db.cursor()
    dumptable(cursor, 'sys.tables', args.target, name='_tables')

    tables = []
    for dbrow in cursor.execute("select name from sys.tables"):
        tables.append(dbrow.name)

    for t in tables:
        dumptable(cursor, t, args.target)

if __name__ == "__main__":
    main()
