import io
import sit.copy
import sit.connectors
import common
import pytest


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
