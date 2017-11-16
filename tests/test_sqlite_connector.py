
from sit.connectors import SQLiteConnector
import textwrap

def test_sqliteconnector():
    """
    test superficially for:

    connector.connect
    connector.exec_query
    connector.insert_data
    connector.commit_transaction
    """
    connector = SQLiteConnector({
        'database': '/tmp/sit_test_sqlite.sqlite'
    })
    connector.connect()

    connector.exec_query(
        textwrap.dedent('''
          CREATE TABLE test_01(
            value INTEGER,
            hex   VARCHAR(20)
          );
          ''')
    )
    connector.commit_transaction()

    connector.insert_data(
        'test_01',
        ('value', 'hex'),
        [(i, hex(i)) for i in range(0, 30)]
    )
    connector.commit_transaction()


    query = connector.prepare_bulk_select_query(
        'test_01',
        ('value', 'hex')
    )

    r = list(connector.execute(query))
    assert r[0] == (0, '0x0')
    assert r[1] == (1, '0x1')

    connector.exec_query('DROP TABLE test_01')
