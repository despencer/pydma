import yaml

class SimpleType:
    def __init__(self):
        self.name = ''

    @classmethod
    def make(cls, name):
        atype = cls()
        atype.name = name
        return atype

class RecordType:
    def __init__(self):
        self.name = ''
        self.members = []

class ReferenceType:
    def __init__(self):
        self.basetype = None

    @classmethod
    def make(cls, basetype):
        rtype = cls()
        rtype.basetype = basetype
        return rtype

class Member:
    def __init__(self):
        self.name = ''
        self.datatype = None

    @classmethod
    def make(cls, name, datatype):
        member = cls()
        member.name = name
        member.datatype = datatype
        return member

class Structure:
    def __init__(self):
        self.name = ''
        self.parent = None
        self.children = []
        self.types = []

    def get_child(self, names):
        for c in self.children:
            if c.name == names[0]:
                if len(names) == 1:
                    return c
                return c.get_child(names[1:])
        c = self
        for n in names:
            cc = Structure()
            cc.name = n
            cc.parent = c
            c.children.append(cc)
            c = cc
        return c

    def get_type(self, name):
        for t in self.types:
            if t.name == name:
                return t
        if self.parent != None:
            return self.parent.get_type(name)
        raise Exception(f'Type {name} not found')

class Package:
    def __init__(self):
        self.name = ''
        self.root = Structure()
        self.version = 1
        self.root.types = [ SimpleType.make('uint32'), SimpleType.make('int32'), SimpleType.make('string') ]

    def get_namespace(self, name):
        if name == None or name == '':
            return self.root
        return self.root.get_child(name.split('.'))

class DbDescriptor:
    def __init__(self):
        self.types = {}

    def make(self, dbpack):
        self.dbpack = dbpack
        self.target = DbPackage()
        self.target.structure.version = dbpack.structure.version
        self.target.structure.name = dbpack.structure.name
        self.target.structure.root.types = []
        for t in set(self.types.values()):
            self.target.structure.root.types.append( SimpleType.make(t) )
        for e in dbpack.entities:
            self.target.entities.append( self.make_entity(e) )
        return self.target

    def make_entity(self, e):
        tarent = RecordType()
        tarent.name = self.dbpack.root.name + '_' + e.name
        for c in e.members:
            tarent.members.append(Member.make(c.name, c.datatype))
        i = 0
        while i < len(tarent.members):
            col = tarent.members[i]
            if isinstance(col.datatype, RecordType):
                tarent.members.pop(i)
                for j, m in enumerate(col.datatype.members):
                    tarent.members.insert(i+j, Member.make(col.name + '_' + m.name, m.datatype) )
            elif isinstance(col.datatype, ReferenceType):
                col.datatype = self.target.structure.root.get_type(self.types['id'])
                i += 1
            else:
                col.datatype = self.target.structure.root.get_type(self.types[col.datatype.name])
                i += 1
        return tarent

    @classmethod
    def sqlite(cls):
        desc = cls()
        desc.types = {'id':'INTEGER', 'int32':'INTEGER', 'uint32':'INTEGER', 'string':'TEXT'}
        return desc

class DbPackage:
    def __init__(self):
        self.structure = Package()
        self.structure.root.types.append( SimpleType.make('id') )
        self.root = None
        self.entities = []

class DbMetaLoader:
    def __init__(self):
        pass

    def loadmember(self, ym, space):
        member = Member()
        if 'reference' in ym:
            member.datatype = ReferenceType.make(space.get_type(ym['reference']))
            member.name = member.datatype.basetype.name
        if 'name' in ym:
            member.name = ym['name']
        if 'type' in ym:
            member.datatype = space.get_type(ym['type'])
        return member

    def loadtype(self, ytype, space):
        atype = RecordType()
        atype.name = ytype['name']
        for ym in ytype['members']:
            atype.members.append(self.loadmember(ym, space))
        space.types.append(atype)
        return atype

    def registerentity(self, yent, dbpackage, space):
        ent = RecordType()
        ent.name = yent['name']
        ent.members.append( Member.make('id', space.get_type('id')) )
        space.types.append(ent)
        dbpackage.entities.append(ent)
        return ent

    def loadentity(self, yent, dbpackage, space):
        for ent in dbpackage.entities:
            if ent.name == yent['name']:
                if 'columns' in yent:
                    for yc in yent['columns']:
                        ent.members.append(self.loadmember(yc, space))
                return ent
        return None

    def loadmeta(self, filename):
        dbpack = DbPackage()
        with open(filename) as strfile:
            ystr = yaml.load(strfile, Loader=yaml.Loader)['package']
            dbpack.structure.name = ystr['name']
            if 'version' in ystr:
                dbpack.structure.version = ystr['version']
            space = dbpack.structure.get_namespace(ystr['namespace'])
            dbpack.root = space
            for yt in ystr['types']:
                self.loadtype(yt, space)
            for ye in ystr['entities']:
                self.registerentity(ye, dbpack, space)
            for ye in ystr['entities']:
                self.loadentity(ye, dbpack, space)
        return dbpack

def loadmeta(filename):
    return DbMetaLoader().loadmeta(filename)

def sqlite():
    return DbDescriptor.sqlite()
