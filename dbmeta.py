import sqlite3
import logging
from datetime import datetime, timezone

class DbMeta:
    def __init__(self, tablename, fields):
        self.tablename = tablename
        self.fields = fields
        self.selectidstmt = "SELECT {0} FROM {1} WHERE id = ?".format(','.join(self.fields), self.tablename)
        self.insertstmt = "INSERT INTO {0} ({1}) VALUES ({2})".format(self.tablename, ','.join(self.fields), ','.join(map(lambda x:'?', range(len(self.fields)))))

    @classmethod
    def set(cls, factory, tablename, attrs):
        if not hasattr(factory, 'dbmeta'):
            setattr(factory, 'dbmeta', DbMeta(tablename, attrs))

    @classmethod
    def init(cls, factory, obj):
        for a in factory.dbmeta.fields:
            setattr(obj, a, None)

    @classmethod
    def insert(cls, db, factory, obj):
        db.execute(factory.dbmeta.insertstmt, cls.values(factory, obj))
        return obj

    @classmethod
    def get(cls, db, factory, id):
        res = db.execute(factory.dbmeta.selectidstmt, (id, ))
        if(len(res) == 0):
            return None
        return cls.fromvalues(factory, res[0])

    @classmethod
    def fromvalues(cls, factory, values):
        obj = factory()
        for n,v in zip(factory.dbmeta.fields, values):
           setattr(obj, n, v)
        return obj

    @classmethod
    def values(cls, factory, obj):
       vals = []
       for n in factory.dbmeta.fields:
            vals.append(getattr(obj, n))
       return tuple(vals)

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
                return True
            except:
                self.db.db.rollback()
                raise
        return False

class Db:
    def __init__(self, name):
        self.filename = name
        if self.filename.find('.') < 0:
                self.filename += '.db'

    def open(self):
        self.db = sqlite3.connect(self.filename)
        self.cur = self.db.cursor()
        self.cur.execute("PRAGMA foreign_keys = ON")
        DbPackaging(self).checkregistry()

    def close(self):
        self.db.close()
        self.db = None
        self.cur = None

    def run(self):
        return DbRun(self)

    def deploypacket(self, name, version, script):
        return DbPackaging(self).deploypacket(name, version, script)

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

    def finish(self, success=True):
        if success:
            self.db.db.commit()
        else:
            self.db.db.rollback()

    def __enter__(self):
        return self

    def __exit__(self, extype, exvalue, extrace):
        self.finish(extype == None)
