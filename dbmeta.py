import sqlite3
import logging
from datetime import datetime, timezone

class DbPackaging:
    def __init__(self):
        pass

    def checkregistry(self):
        try:
            self.cur.execute("SELECT * FROM dbpacket WHERE module = 'init'")
        except sqlite3.OperationalError:
            self.cur.execute("CREATE TABLE dbpacket (module TEXT NOT NULL, version INTEGER NOT NULL, deploy INTEGER, PRIMARY KEY (module, version))")
            self.db.commit()
            logging.info("Packet registry created")

    def ispacketregistered(self, name, version):
        self.cur.execute("SELECT deploy FROM dbpacket WHERE module = ? AND version = ?", ( name ,version))
        if self.cur.fetchone() == None:
            return False
        logging.debug('Row fetched from deploy for %s-%s', name, version)
        return True

    def registerpacket(self, name, version):
        time = datetime.now(timezone.utc)
        self.cur.execute("INSERT INTO dbpacket (module, version, deploy) VALUES (?, ?, ?)", (name, version, int(datetime.now(timezone.utc).timestamp())))
        logging.info('Packet %s-%s registered', name, version)

    def deploypacket(self, name, version, script):
        if not self.ispacketregistered(name, version):
            try:
                if isinstance (script, list):
                    for s in script:
                        self.cur.execute(s)
                else:
                    self.cur.execute(script)
                self.registerpacket(name, version)
                self.db.commit()
                self.logger.info('Packet %s version %s deployed', name, version)
            except:
                self.db.rollback()
                raise

    def open(self, name):
        self.db = sqlite3.connect(name + '.db')
        self.cur = self.db.cursor()
        self.checkregistry()

    def close(self):
        self.db.close()

