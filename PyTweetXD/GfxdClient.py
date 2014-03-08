# -*- coding: utf-8 -*-
'''
Created on Nov 28, 2013
@author: markito
'''
import DB2
from ConfigParser import ConfigParser

'''
GemFireXD client example using pyDB2
@see: Check config file for database properties 
'''
class GfxdClient():
    _cursor = None
    _conn = None
    _config = ConfigParser()
    
    def __init__(self):
        self._config.read('config')
    
    def readConfig(self, name):
        return self._config.get('GemFireXD',name)
    
    def connect(self):
        self._conn = DB2.connect(self.readConfig('database'), self.readConfig('user'), self.readConfig('password'))
    
    def getCursor(self):
        if (self._cursor == None) & (self._conn != None):
                _cursor = self._conn.cursor()
                return _cursor
        else:
            raise Exception("No active connection!")
        

    def execute(self, sql, params = None):
        if (params):
            self.getCursor().execute(sql, params)
        else:
            self.getCursor().execute(sql)
            
        return self.getCursor()
        
    def select(self, sql, params = None):
        return self.execute(sql, params)
    
    def insert(self, sql, params):
        return self.execute(sql, params) 
 
