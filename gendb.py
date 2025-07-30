#!/usr/bin/python3
import os
import jinja2
import dbdatadef

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Database access layer generator')
    parser.add_argument('dbmeta', help="The database structure filename", type=str)
    parser.add_argument('target', help="The target python filename", type=str)
    args = parser.parse_args()
    dbstr = dbdatadef.loadmeta(args.dbmeta)
    dbstr = dbdatadef.sqlite().make(dbstr)
    env = jinja2.Environment(loader=jinja2.FileSystemLoader( os.path.dirname(__file__) ))
    with open(args.target , mode='w') as target:
        target.write(env.get_template('db.py.jinja').render(package=dbstr))

