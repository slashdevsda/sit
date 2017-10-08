#
# connector are simple abtraction over database engines.
#
import re
import itertools
import logging
import sqlite3
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

    def exec_query(self, q):
        r = self.cursor.execute(q)
        if r and self.cursor.description:
            out = ', '.join([i[0] for i in self.cursor.description])
            out += '\n'
            out += '\n'.join([str(i) for i in r])
            return out
        else:
            return ''

    def prepare_bulk_select_query(self, table_name, columns):
        q = 'SELECT {} FROM {};'.format(
            ', '.join(columns),
            table_name
        )
        return q



    #@abstractmethod
    def fetch_data(self, table, columns, force_query=False, **args):
        '''
        generator
        '''
        q = force_query if force_query else self.prepare_bulk_select_query(table, columns)

        rows = self.cursor.execute(q)
        header = map(lambda x:x[0],  self.cursor.description)
        yield list(header)

        for item in rows:
            yield item


    # Common features for each driver
    ## bulk data insertion / query
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
        except sqlite3.OperationalError as e:
            log.exception(e)
            log.warn('table %s already exists', table_name)
            return False

    def prepare_bulk_insert_query(self, table_name, line_list, columns):
        '''
        Construct a query in the form.
        INSERT INTO <table_name>VALUES (...).

        Trying to make a good use of db-api2.0
        '''
        q = 'INSERT INTO {}({})\nVALUES \n'.format(table_name, ', '.join(columns))

        q += ',\n'.join([
            '({})'.format(', '.join(["?" for i in columns]))
            for i in line_list
        ])
        return q


    def insert_data(self, table, columns, data, **args):
        q = self.prepare_bulk_insert_query(table, data, columns)
        self.cursor.execute(q, tuple(itertools.chain.from_iterable(data)))




#
# SQL-Server (pymssql)
#

import pymssql

class MSSQLConnector(Connector):
    def __init__(self, args):
        self.args = args
        super().__init__()

    def connect(self, **args):
        '''
        connect to Microsoft© SQL-Server™
        '''
        self.conn = pymssql.connect(
            user=self.args['user'],
            password=self.args['password'],
            host=self.args['hostname'],
            database=self.args['database'],
            port=self.args['port']
        )
        self.cursor = self.conn.cursor()

    def exec_query(self, q):
        self.cursor.execute(q)
        rows = self.cursor.fetchall()

        out = ', '.join([i[0] for i in self.cursor.description])
        out += '\n'
        out += '\n'.join([str(i) for i in rows])
        return out

    def commit_transaction(self):
        self.conn.commit()

    def rollback_transaction(self):
        self.conn.rollback()

    def prepare_bulk_insert_query(self, table_name, line_list, columns):
        '''
        Overload `prepare_bulk_insert_query` for pymssql sql
        specific details.

        Construct a query in the form.
        INSERT INTO <table_name>VALUES (...).
        '''
        q = 'INSERT INTO {}({})\nVALUES \n'.format(table_name, ', '.join(columns))

        q += ',\n'.join([
            '({})'.format(', '.join(["%s" for i in columns]))
            for i in line_list
        ])
        return q



#
# Sqlite (sqlite3)
#

class SQLiteConnector(Connector):
    '''
    this connector should always be available since sqlite3
    is part of the standard python distribution.
    '''
    def __init__(self, args):
        self.args = args
        super().__init__()

    def connect(self, **args):
        '''
        connect to sqlite
        '''
        self.conn = sqlite3.connect(self.args['database'])
        self.cursor = self.conn.cursor()

    def commit_transaction(self):
        self.conn.commit()

    def rollback_transaction(self):
        self.conn.rollback()



CONNECTORS = {
    'sqlserver': MSSQLConnector if MSSQLConnector else None,
    'sqlite': SQLiteConnector,
}

def get_connector(args):
    '''
    choose the right connector depending on
    program arguments (`args`)
    '''
    c = CONNECTORS.get(args['driver'])
    return c(args)
