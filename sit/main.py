import argparse
import logging
import configparser
import os.path
import sys
import textwrap

import sit.connectors as connectors
from sit.ui import Shell
import sit.copy

log = logging.getLogger(__name__)


def raw_mode(args, connector):
    '''
    enterring raw mode
    '''

    log.debug('enterring raw mode')
    try:
        Shell(connector).loop()
    except (EOFError, KeyboardInterrupt):
        print('exiting ...')
        exit(0)



def copy_mode(config, connector):
    '''
    copy to database
    '''
    args = config['args']
    if args.get('file'):
        fd = open(args['file'], 'r')
    else:
        fd = sys.stdin

    if args.get('table_name'):
        table_name = args['table_name']
    else:
        table_name = str(int(time.time()))

    try:
        total = sit.copy.copy_from_fd(
            table_name,
            fd,
            connector,
            create_table=args.get('create_table')
        )
        connector.commit_transaction()
        log.info('%s rows inserted.', total)
    except Exception as e:
        log.exception('Exception occured, rollback now.')
        connector.rollback_transaction()
        sys.exit(2)


def pull_mode(config, connector):
    '''
    copy from database
    '''
    args = config['args']
    if args.get('file'):
        fd = open(args['file'], 'w+')
    else:
        fd = sys.stdout

    if args.get('table_name') or args.get('query'):
        table_name = args.get('table_name')
    else:
        log.error('need at leat a table name (-T) or a custom query (-q)')
        exit(1)

    try:
        total = sit.copy.copy_to_fd(
            table_name,
            fd,
            connector,
            force_query=args.get('query')
        )
    except Exception as e:
        log.exception('Exception occured.')
        sys.exit(2)



def read_config(args):
    if os.path.isfile('./sit.ini'):
        path = './sit.ini'
    elif os.path.isfile('~/.sit.ini'):
        path = '~/.sit.ini'
    elif args.get('config'):
        if os.path.isfile(args['config']):
            path = args['config']
        else:
            log.error('{}: bad config path'.format(args['config']))
            exit(1)
    else:
        # no config file available
        return {}

    config = configparser.ConfigParser()
    config.read(path)
    config['DEFAULT'] = config[config.sections()[0]]
    return dict(**config)


class InvalidConfig(Exception):
    pass

def validate_config(c):
    '''
    quickly validate config or current env
    '''
    valid_driver_list = ('sqlserver', 'sqlite')
    # todo: deport to connector
    if c['current'].get('driver') == 'sqlserver':
        if not c['current'].get('hostname'):
            raise InvalidConfig('missing hostname')
        if not c['current'].get('user'):
            raise InvalidConfig('missing user')
        if not c['current'].get('password'):
            raise InvalidConfig('missing password')
        if not c['current'].get('database'):
            raise InvalidConfig('missing database')
        if not c['current'].get('port'):
            raise InvalidConfig('missing port')
        if not c['current'].get('driver'):
            raise InvalidConfig('missing driver')

    elif c['current'].get('driver') == 'sqlite':
        if not c['current'].get('database'):
            raise InvalidConfig('missing database')
    else:
        raise InvalidConfig(
            'invalid driver. Available choices are ({})'.format(
                ', '.join(valid_driver_list)
            )
        )



def start():
    '''
    Program entry point
    '''
    parser = argparse.ArgumentParser(
    prog='sit',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-c', '--config',
        help=(
            'read configuration from file (default to ./sit.ini, '
            'fallback on ~/.sit.ini'
            )
    )
    parser.add_argument('--verbose', action='store_true',
                            help='set DEBUG logging level')
    parser.add_argument('-u', '--user', help='username used')
    parser.add_argument('-e', '--engine', help='db engine/driver')
    parser.add_argument('-p', '--password', help='password used')
    parser.add_argument('-t', '--hostname', help='database hostname (IP are ok)')
    parser.add_argument('-d', '--database', help='which database to connect to')
    parser.add_argument( '--port', help='service port')

    subparsers = parser.add_subparsers(help='* sub-commands help *')

    #  parser for the "raw" command
    parser_raw = subparsers.add_parser(
        'raw',
        help=(
            'open an SQL prompt to the remote server.'
            'You\'ll need `pygment` and `prompt_toolkit`.')
    )
    parser_raw.set_defaults(func=raw_mode)

    #  parser for the "copy to" command
    parser_to = subparsers.add_parser(
        'push',
        formatter_class=argparse.RawTextHelpFormatter,
        help=(
            'Copy data to remote server. Can take regular CSV'
            ' directly from STDIN.\n')
    )
    parser_to.add_argument(
        '-f', '--file', help='read from file instead of stdin'
    )
    parser_to.add_argument(
        '-T', '--table', dest='table_name', nargs='?',
        help='INSERT into this table (default to current timestamp)'
    )
    parser_to.add_argument(
        '-C', '--create', dest='create_table',
        action='store_true',
        help=textwrap.dedent('''\
            automatically attempt to create a new table.
            Column types will be naively inferred from first
            lines of data.
            eg:
            `$ sit push -f dummy.csv -CT dummy_insert dev`
'''
        )
    )

    parser_to.set_defaults(func=copy_mode)


    #  parser for the "copy from" command
    parser_from = subparsers.add_parser(
        'pull',
        formatter_class=argparse.RawTextHelpFormatter,
        help=(
            'Retrieve data from remote server. Outputs regular CSV'
            ' on stdout')
    )
    parser_from.add_argument(
        '-f', '--file', help='write to file instead of stdout'
    )
    parser_from.add_argument(
        '-T', '--table', dest='table_name', nargs='?',
        help='extract data from this table'
    )
    parser_from.add_argument(
        '-q', '--query', dest='query',
        help=textwrap.dedent('''
            run a custom sqlquery for data retrieval.

            example:

            $ sit pull -q 'SELECT *
            > FROM users U
            > ORDER BY U.points
            > LIMIT 100' staging > top_100_users.csv
            '''),
        nargs='?'
    )

    parser_from.set_defaults(func=pull_mode)

    parser.add_argument('env', help='environement')

    args = parser.parse_args()

    # setup logging
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)


    # converting arguments
    # to a regular dictionnary
    arguments = vars(args)

    config = read_config(arguments)
    if arguments['env'] not in config:
        log.error(
            'no such environement: {}. Availables envs are: {}.'.format(
                arguments['env'],
                ', '.join(config.keys()),
            )
        )
        exit(1)
    config['current'] = dict(config[arguments['env']])

    # merge command line arguments
    config['current'].update({
        i:arguments[i]
        for i in [
            'hostname', 'user', 'password', 'port',
            'engine', 'database'
        ]
        if arguments.get(i)
    })
    config['args'] = {
        i:arguments[i]
        for i in arguments if arguments.get(i)
    }

    validate_config(config)
    log.debug(
        'working on %s (%s)',
        config['current']['database'],
        config['current']['driver']
    )

    connector = connectors.get_connector(config['current'])
    connector.connect()
    if hasattr(args, 'func'):
        args.func(config, connector)


if __name__ == '__main__':
    parse_args()
