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

