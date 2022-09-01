import sqlite3
import logging
from datetime import datetime, timezone

class DbPackaging:
    def __init__(self, db):
        self.db = db

    def checkregistry(self):
        try:
            self.db.cur.execute("SELECT * FROM dbm_packet WHERE module = 'init'")
        except sqlite3.OperationalError:
            self.db.cur.execute("CREATE TABLE dbm_packet (module TEXT NOT NULL, version INTEGER NOT NULL, deploy INTEGER, PRIMARY KEY (module, version))")
            self.db.db.commit()
            logging.info("Packet registry created")
            self.deploypacket('seqid', 1,"CREATE TABLE seqid_seq (id INTEGER NOT NULL)")
            self.db.cur.execute("INSERT INTO seqid_seq (id) VALUES (?)", (1, ) )
            self.db.db.commit()

    def ispacketregistered(self, name, version):
        self.db.cur.execute("SELECT deploy FROM dbm_packet WHERE module = ? AND version = ?", ( name ,version))
        if self.db.cur.fetchone() == None:
            return False
        logging.debug('Row fetched from deploy for %s-%s', name, version)
        return True

    def registerpacket(self, name, version):
        time = datetime.now(timezone.utc)
        self.db.cur.execute("INSERT INTO dbm_packet (module, version, deploy) VALUES (?, ?, ?)", (name, version, int(datetime.now(timezone.utc).timestamp())))
        logging.info('Packet %s-%s registered', name, version)

    def deploypacket(self, name, version, script):
        if not self.ispacketregistered(name, version):
            try:
                if isinstance (script, list):
                    for s in script:
                        self.db.cur.execute(s)
                else:
                    self.db.cur.execute(script)
                self.registerpacket(name, version)
                self.db.db.commit()
                logging.info('Packet %s version %s deployed', name, version)
            except:
                self.db.db.rollback()
                raise

class Db:
    def __init__(self, name):
        self.filename = name
        if self.filename.find('.') < 0:
                self.filename += '.db'

    def open(self):
        self.db = sqlite3.connect(self.filename)
        self.cur = self.db.cursor()
        DbPackaging(self).checkregistry()

    def close(self):
        self.db.close()
        self.db = None
        self.cur = None

    def run(self):
        return DbRun(self)

    def deploypacket(self, name, version, script):
        DbPackaging(self).deploypacket(name, version, script)

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, extype, exvalue, extrace):
        self.close()

class DbRun:
    def __init__(self, db):
        self.db = db

    def genid(self):
        self.db.cur.execute("SELECT id FROM seqid_seq")
        id = self.db.cur.fetchone()[0]
        self.db.cur.execute("UPDATE seqid_seq SET id = ?", (id+1, ) )
        return id

    def execute(self, script, values):
        self.db.cur.execute(script, values)
        return self.db.cur.fetchall()

    def __enter__(self):
        return self

    def __exit__(self, extype, exvalue, extrace):
        if extype == None:
            self.db.db.commit()
        else:
            self.db.db.rollback()
