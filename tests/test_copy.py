import io
import sit.copy
import sit.connectors
import common
import pytest
import textwrap


@pytest.fixture(scope="module")
def test_connector():
    """
    instanciate an SQLite connector in order
    to test core features.

    connectors should be tested appart
    """
    connector = sit.connectors.SQLiteConnector({
        'database': '/tmp/sit_test.sqlite'
    })
    connector.connect()
    yield connector
    connector.exec_query('DROP TABLE unitest')
    connector.exec_query('DROP TABLE unitest_enc')
    connector.exec_query('DROP TABLE unitest_test_custom_fetch')


def test_pipeline(test_connector):
    '''
    Insert CSV, then retrieve it using sit.copy_to_fd,
    and compare it to the original piece of CSV.

    This example does not reflect indempotence,
    since some types (floats?) could eventually loose
    some precision (TODO: what happen with floats)
    '''
    fd = io.StringIO(common.TEST_CSV)
    sit.copy.copy_from_fd(
        'unitest',
        fd,
        test_connector,
        create_table=True
    )
    test_connector.commit_transaction()

    # now, retrieval
    fd_out = io.StringIO()

    sit.copy.copy_to_fd(
        'unitest',
        fd_out,
        test_connector,
    )
    out_csv = fd_out.getvalue().replace('\r\n', '\n')
    assert out_csv == common.TEST_CSV


def test_push_unicode_csv(test_connector):
    '''
    Insert CSV, then retrieve it using sit.copy_to_fd,
    and compare it to the original piece of CSV.

    This example does not reflect indempotence,
    since some types (floats?) could eventually loose
    some precision (TODO: what happen with floats)
    '''

    fd = io.StringIO(common.U_CSV)
    sit.copy.copy_from_fd(
        'unitest_enc',
        fd,
        test_connector,
        create_table=True
    )
    test_connector.commit_transaction()

    # retrieval
    fd_out = io.StringIO()

    sit.copy.copy_to_fd(
        'unitest_enc',
        fd_out,
        test_connector,
    )

    out_csv = fd_out.getvalue().replace('\r\n', '\n')
    assert out_csv == common.U_CSV


def test_custom_retrieval_query(test_connector):
    '''
    Insert CSV, then retrieve it using sit.copy_to_fd,
    and compare it to the original piece of CSV.

    This example does not reflect indempotence,
    since some types (floats?) could eventually loose
    some precision (TODO: what happen with floats)
    '''
    fd = io.StringIO(common.TEST_CSV)
    sit.copy.copy_from_fd(
        'unitest_test_custom_fetch',
        fd,
        test_connector,
        create_table=True
    )
    test_connector.commit_transaction()
    # retrieval
    fd_out = io.StringIO()

    sit.copy.copy_to_fd(
        'unitest_enc',
        fd_out,
        test_connector,
        force_query=textwrap.dedent('''
        SELECT zip FROM unitest_test_custom_fetch
        WHERE street = '6001 MCMAHON DR'
        AND city = 'SACRAMENTO'
        ''')
    )
    out_csv = fd_out.getvalue().replace('\r\n', '\n')
    assert out_csv == textwrap.dedent('''\
        zip
        95824
        ''')
