# -*- coding: utf-8 -*-
#TODO use zope.interfaces, as in twisted
class Mapper_Template(object):
    pass

class Reducer_Template(object):
    pass

class Reader(object):
    def getKeyValue(self):
        raise RuntimeError("Please, define a reader")()


