import sqlite3
import logging
from datetime import datetime, timezone, timedelta
from threading import Thread

class Indicator:
    def __init__(self, interval=10):
        self.start = datetime.now()
        self.interval = timedelta(seconds = interval)

    def ready(self):
        delta = datetime.now()-self.start
        if delta >= self.interval:
            self.start = datetime.now()
            return True
        return False

class Runner:
    def __init__(self, processor):
        self.stop = False
        self.processor = processor

    def main(self):
        self.processor.open()
        indy = Indicator()
        while not self.stop:
            if indy.ready():
                self.processor.info()
            self.processor.step()
        self.processor.close()

    @classmethod
    def run(cls, processor):
        print('Press enter to stop')
        runner = cls(processor)
        t = Thread(target=runner.main)
        t.start()
        input()
        runner.stop = True


class DbMeta:
    def __init__(self, tablename, fields, readers, writers):
        self.tablename = tablename
        self.fields = fields
        self.readers = readers
        self.writers = writers
        self.selectidstmt = "SELECT {0} FROM {1} WHERE id = ?".format(','.join(self.fields), self.tablename)
        self.insertstmt = "INSERT INTO {0} ({1}) VALUES ({2})".format(self.tablename, ','.join(self.fields), ','.join(map(lambda x:'?', range(len(self.fields)))))
        self.updateidstmt = "UPDATE {0} SET {1} WHERE id = ?".format(self.tablename, ','.join( map(lambda x:x+'=?', self.fields)) )

    @classmethod
    def set(cls, factory, tablename, attrs, readers={}, writers={}):
        if not hasattr(factory, 'dbmeta'):
            setattr(factory, 'dbmeta', DbMeta(tablename, attrs, readers, writers))

    @classmethod
    def init(cls, obj):
        factory = obj.__class__
        for a in factory.dbmeta.fields:
            setattr(obj, a, None)
        if not hasattr(obj.__class__, 'update'):
            setattr(obj.__class__, 'update', lambda s,db: cls.update(db, s))

    @classmethod
    def insert(cls, db, obj):
        factory = obj.__class__
        db.execute(factory.dbmeta.insertstmt, cls.values(factory, obj))
        return obj

    @classmethod
    def update(cls, db, obj):
        factory = obj.__class__
        db.execute(factory.dbmeta.updateidstmt, (*cls.values(factory, obj), obj.id) )

    @classmethod
    def get(cls, db, factory, id):
        return cls.selectone(db, factory, factory.dbmeta.selectidstmt, id)

    @classmethod
    def getby(cls, db, factory, condition, *args):
        return cls.selectone(db, factory, "SELECT {0} FROM {1} WHERE {2}".format(','.join(factory.dbmeta.fields), factory.dbmeta.tablename, condition), *args)

    @classmethod
    def getlist(cls, db, factory, condition, *args):
        return cls.selectlist(db, factory, "SELECT {0} FROM {1} WHERE {2}".format(','.join(factory.dbmeta.fields), factory.dbmeta.tablename, condition), *args)

    @classmethod
    def selectone(cls, db, factory, stmt, *args):
        res = db.execute(stmt, args)
        if(len(res) == 0):
            return None
        return cls.fromvalues(factory, res[0])

    @classmethod
    def selectlist(cls, db, factory, stmt, *args):
        return list(map( lambda x:cls.fromvalues(factory,x), db.execute(stmt, args) ))

    @classmethod
    def fromvalues(cls, factory, values):
        obj = factory()
        for n,v in zip(factory.dbmeta.fields, values):
           if n in factory.dbmeta.readers:
                v = factory.dbmeta.readers[n](v)
           setattr(obj, n, v)
        return obj

    @classmethod
    def values(cls, factory, obj):
       vals = []
       for n in factory.dbmeta.fields:
            v = getattr(obj, n)
            if n in factory.dbmeta.writers:
                v = factory.dbmeta.writers[n](v)
            vals.append(v)
       return tuple(vals)

    @classmethod
    def now(cls):
        return int(datetime.now(timezone.utc).timestamp())

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
    def __init__(self, name, structure):
        self.filename = name
        if self.filename.find('.') < 0:
                self.filename += '.db'
        self.structure = structure

    def open(self):
        self.db = sqlite3.connect(self.filename)
        self.cur = self.db.cursor()
        self.cur.execute("PRAGMA foreign_keys = ON")
        DbPackaging(self).checkregistry()
        self.structure(self)

    def close(self):
        self.db.close()
        self.db = None
        self.cur = None

    def deploypacket(self, name, version, script):
        return DbPackaging(self).deploypacket(name, version, script)

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, extype, exvalue, extrace):
        self.finish(extype == None)
        self.close()

    def genid(self):
        self.cur.execute("SELECT id FROM seqid_seq")
        id = self.cur.fetchone()[0]
        self.db.execute("UPDATE seqid_seq SET id = ?", (id+1, ) )
        return id

    def execute(self, script, values):
        self.cur.execute(script, values)
        return self.cur.fetchall()

    def finish(self, success=True):
        if success:
            self.db.commit()
        else:
            self.db.rollback()
