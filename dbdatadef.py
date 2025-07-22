import yaml


class DbPackage:
    def __init__(self):
        self.entities = {}

    @classmethod
    def loadmeta(cls, filename):
        package = cls()
        with open(filename) as strfile:
            ystr = yaml.load(strfile, Loader=yaml.Loader)
        return package

def loadmeta(filename):
    return DbPackage.loadmeta(filename)
