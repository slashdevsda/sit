#
# COPY
#
# read input CSV
# deserialise
# INSERT

import sys
import csv
import logging

log = logging.getLogger(__name__)



def create_dest_table(tbn, columns, row, connector):

    table_name = tbn if tbn else str(int(time.time()))
    connector.create_table(table_name, columns, row)

def copy_from_fd(table_name, fd, connector,
                     auto_detect=True,
                     create_table=True,
                     buffer_row=20):
    '''
    transfert data from a filedescriptor to
    connector.

    filedescriptor must point to an opened file
    (or socket) containing/streaming CSV.

    First row should include columns names.

    return the number of row inserted
    '''

    dialect = csv.excel
    reader = csv.DictReader(fd)

    b = []
    i, total = 0, 0

    for row in reader:
        b.append([row[i] for i in reader.fieldnames])

        # table
        if total == 0 and create_table:
            create_dest_table(
                table_name,
                reader.fieldnames,
                # we need ordering
                [row[i] for i in reader.fieldnames],
                connector
            )
        i += 1
        total += 1
        if i >= buffer_row:
            print('\r%d' %total, end='')
            sys.stdout.flush()
            i = 0
            connector.insert_data(table_name, reader.fieldnames, b)
            b = []


    # if buffer still contains data
    # at the end of file
    if len(b):
        connector.insert_data(table_name, reader.fieldnames, b)


    print('\r%d.' %total, end='')
    print('\n')
    return total


def copy_to_fd(table_name, fd, connector, arglist="*", force_query=False):
    '''
    '''
    writer = csv.writer(fd)
    writer.writerows(
        connector.fetch_data(table_name, arglist, force_query=force_query)
    )
