#
# connector are simple abtraction over database engines.
#
import re
import itertools
import logging
from abc import ABC, abstractmethod


log = logging.getLogger(__name__)

class TableAlreadyExists(Exception):
    pass

class Connector(ABC):

    @abstractmethod
    def connect(self, **args):
        pass

    @abstractmethod
    def commit_transaction(self):
        pass

    @abstractmethod
    def rollback_transaction(self):
        pass

    ##
    ## Query preparation
    ##

    @abstractmethod
    def prepare_bulk_insert_query(line_list, columns):
        raise NotImplemented()

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

    def prepare_bulk_insert_query(line_list, columns):
        raise NotImplemented()



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

    ## utils
    def determine_columns_type(self, columns, row):
        '''
        `row` is a list of values
        '''
        columns_with_types = []
        for colname, item in zip(columns, row):
            if item.isnumeric():
                columns_with_types.append((colname, 'INTEGER'))
            elif re.match("^(-)?\d+?\.\d+?$", item):
                columns_with_types.append((colname, 'FLOAT'))
            else:
                # todo: find a better solution for strings
                # (automatic alters ?)
                columns_with_types.append((colname, 'VARCHAR(255)'))
        return columns_with_types


    def create_table(self, table_name, columns, init_data):
        '''
        [sqlite]
        Create a table on-demand for a specific
        import.

        This function does not trigger transaction
        commitment, so the table is created
        only if something commits changes after its execution.
        '''

        cols = self.determine_columns_type(columns, init_data)
        q = "CREATE TABLE {}(\n".format(table_name)
        for idx, (name, typ) in enumerate(cols):
            q += "\t{} {}".format(name, typ)
            if idx != len(cols) - 1:
                q += ','
            q += '\n'
        q += ');\n'

        try:
            self.exec_query(q)
            return True
        except sqlite3.OperationalError:
            log.warn('table %s already exists', table_name)
            return False

    def prepare_bulk_insert_query(self, table_name, line_list, columns):
        q = 'INSERT INTO {}({})\nVALUES \n'.format(table_name, ', '.join(columns))

        q += ',\n'.join([
            '({})'.format(', '.join(["?" for i in columns]))
            for i in line_list
        ])

        return q


    def insert_data(self, table, columns, data, **args):
        q = self.prepare_bulk_insert_query(table, data, columns)
        #print(q)
        self.cursor.execute(q, list(itertools.chain.from_iterable(data)))
        pass


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
