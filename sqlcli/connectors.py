#
# connector are simple abtraction over database engines.
#

from abc import ABC, abstractmethod

class Connector(ABC):

    @staticmethod
    def arguments_required():
        return ['user', 'password', 'hostname', 'database', 'port']

    @abstractmethod
    def connect(self, **args):
        pass

    @abstractmethod
    def commit_transaction(self):
        pass

    @abstractmethod
    def rollback_transaction(self):
        pass

    #@abstractmethod
    def read_table(self, n, select_list='*', **args):
        pass

    # object management
    #@abstractmethod
    def retrieve_object(self, objname, **args):
        pass

    #@abstractmethod
    def update_object(self, objname, content, **args):
        pass

    #@abstractmethod
    def delete_object(self, objname, **args):
        pass

    @abstractmethod
    def exec_query(self, q):
        r = self.cursor.execute(q)
        return r

    # bulk data insertion / query

    @abstractmethod
    def insert_data(self, table, columns, data, **args):
        pass

    #@abstractmethod
    def fetch_data(self, table, columns, offset, limit, **args):
        pass


#
# SQL-Server
#

import pymssql
class MSSQLConnector(Connector):
    def __init__(self, args):
        self.args = args
        super().__init__()

    def connect(self, **args):
        '''
        connect to MS© SQL-Server™
        '''
        self.pymssql = pymssql.connect(
            user=self.args['user'],
            password=self.args['password'],
            host=self.args['hostname'],
            database=self.args['database'],
            port=self.args['port']
        )
        self.cursor = self.pymssql.cursor(as_dict=True)

    def exec_query(self, q):
        r = self.cursor.execute(q)
        return r

    def commit_transaction(self):
        self.cursor.commit()

    def rollback_transaction(self):
        self.cursor.rollback()

    # bulk data insertion / query

    def insert_data(self, table, columns, data, **args):
        q = 'INSERT INTO {}({}) '.format(
            table,
            ', '.join(columns)
        )


    def fetch_data(self, table, columns, offset, limit, **args):
        pass



#
# SQL-Server
#

import sqlite3
class SQLiteConnector(Connector):
    def __init__(self, args):
        self.args = args
        super().__init__()

    def connect(self, **args):
        '''
        connect to sqlite
        '''
        self.conn = sqlite3.connect(self.args['database'])
        self.cursor = self.conn.cursor()

    def exec_query(self, q):
        r = self.cursor.execute(q)

        return '\n'.join([str(i) for i in r])

    def commit_transaction(self):
        self.conn.commit()

    def rollback_transaction(self):
        self.conn.rollback()

    # bulk data insertion / query

    def insert_data(self, table, columns, data, **args):
        q = 'INSERT INTO {}({}) '.format(
            table,
            ', '.join(columns)
        )


    def fetch_data(self, table, columns, offset, limit, **args):
        pass


CONNECTORS = {
    'sqlserver': MSSQLConnector,
    'sqlite': SQLiteConnector,
}

def get_connector(args):
    '''
    choose the right connector depending on
    program arguments (`args`)
    '''
    c = CONNECTORS.get(args['driver'])
    return c(args)
