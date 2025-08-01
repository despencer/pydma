import yaml
import datadef

class DbTable:
    def __init__(self):
        self.name = None
        self.primary = None
        self.columns = []

class DbColumn:
    def __init__(self):
        self.name = None
        self.datatype = None
        self.reference = None

    @classmethod
    def make(cls, name, datatype):
        col = cls()
        col.name = name
        col.datatype = datatype
        return col

class DbPackage:
    def __init__(self):
        self.name = None
        self.version = None
        self.database = None
        self.tables = []

class DbDescriptor:
    def __init__(self):
        self.types = {}

    def make(self, datapack):
        self.datapack = datapack
        self.target = DbPackage()
        self.target.version = datapack.version
        self.target.name = datapack.name
        self.target.database = self
        for e in datapack.root.children[0].types:
            self.target.tables.append( self.make_table(e, datapack.root.children[0] ) )
        return self.target

    def make_table(self, e, space):
        table = DbTable()
        table.name = space.name + '_' + e.name
        for m in e.members:
            if m.name == 'id':
                table.primary = DbColumn.make(m.name, self.types[m.datatype.name])
            else:
                table.columns.append( DbColumn.make(m.name, m.datatype) )
        i = 0
        while i < len(table.columns):
            col = table.columns[i]
            if isinstance(col.datatype, datadef.RecordType):
                table.columns.pop(i)
                for j, m in enumerate(col.datatype.members):
                    table.columns.insert(i+j, DbColumn.make(col.name + '_' + m.name, m.datatype) )
            elif isinstance(col.datatype, datadef.ReferenceType):
                col.reference = space.name + '_' + col.datatype.basetype.name
                col.datatype = self.types['id']
                i += 1
            elif isinstance(col.datatype, datadef.SimpleType):
                col.datatype = self.types[col.datatype.name]
                i += 1
            else:
                i += 1
        return table

    @classmethod
    def sqlite(cls):
        desc = cls()
        desc.types = {'id':'INTEGER', 'int32':'INTEGER', 'uint32':'INTEGER', 'string':'TEXT'}
        return desc

class DbMetaLoader:
    def __init__(self):
        pass

    def loadmember(self, ym, space):
        member = datadef.Member()
        if 'reference' in ym:
            member.datatype = datadef.ReferenceType.make(space.get_type(ym['reference']))
            member.name = member.datatype.basetype.name
        if 'name' in ym:
            member.name = ym['name']
        if 'type' in ym:
            member.datatype = space.get_type(ym['type'])
        return member

    def loadtype(self, ytype, space):
        atype = datadef.RecordType()
        atype.name = ytype['name']
        for ym in ytype['members']:
            atype.members.append(self.loadmember(ym, space))
        space.types.append(atype)
        return atype

    def registerentity(self, yent, space):
        ent = datadef.RecordType()
        ent.name = yent['name']
        ent.members.append( datadef.Member.make('id', space.get_type('id')) )
        space.types.append(ent)
        return ent

    def loadentity(self, yent, space):
        for ent in space.types:
            if ent.name == yent['name']:
                if 'columns' in yent:
                    for yc in yent['columns']:
                        ent.members.append(self.loadmember(yc, space))
                return ent
        return None

    def loadmeta(self, filename):
        pack = datadef.Package()
        pack.root.types.append( datadef.SimpleType.make('id') )
        with open(filename) as strfile:
            ystr = yaml.load(strfile, Loader=yaml.Loader)['package']
            pack.name = ystr['name']
            if 'version' in ystr:
                pack.version = ystr['version']
            space = pack.get_namespace(ystr['namespace'])
            for yt in ystr['types']:
                self.loadtype(yt, pack.root)
            for ye in ystr['entities']:
                self.registerentity(ye, space)
            for ye in ystr['entities']:
                self.loadentity(ye, space)
        return pack

def loadmeta(filename):
    return DbMetaLoader().loadmeta(filename)

def sqlite():
    return DbDescriptor.sqlite()
