#
# SQLCLI
#

# Automate your workflow with databases

# sqlcli raw


import argparse
import logging
import sqlcli.connectors as connectors
from sqlcli.ui import Shell

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

def raw_mode(args, connector):
    '''
    enterring raw mode
    '''

    print('enterring raw mode')
    Shell(connector).loop()


def edit_mode(args, connector):
    '''
    enterring edition mode
    '''
    print('enterring edition mode')


import configparser
import os.path
import sys

def read_config(args):
    if os.path.isfile('./sqlcli.ini'):
        path = './sqlcli.ini'
    elif os.path.isfile('~/.sqlcli.ini'):
        path = '~/.sqlcli.ini'
    elif args.get('config'):
        if os.path.isfile(args['config']):
            path = args['config']
        else:
            print('{}: bad config path'.format(args['config']),
                      file=sys.stderr)
    else:
        # no config file available
        return {}

    config = configparser.ConfigParser()
    config.read(path)
    return dict(**config)


class InvalidConfig(Exception):
    pass

def validate_config(c):
    '''
    quickly validate config or current env
    '''
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
        raise InvalidConfig('missing valid driver.(sqlite/sqlserver)')


def parse_args():
    parser = argparse.ArgumentParser(
    prog='sqlcli',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-c', '--config',
        help=(
            'read configuration from file (default to ./sqlcli.ini, '
            'fallback on ~/.sqlcli.ini'
            )
    )
    parser.add_argument('-u', '--user', help='username used')
    parser.add_argument('-e', '--engine', help='db engine/driver')
    parser.add_argument('-p', '--password', help='password used')
    parser.add_argument('-t', '--hostname', help='database hostname (IP are ok)')
    parser.add_argument('-d', '--database', help='which database to connect to')
    parser.add_argument( '--port', help='service port')

    subparsers = parser.add_subparsers(help='* sub-commands help *')

    #  parser for the "edit" command
    parser_edit = subparsers.add_parser(
        'edit',
        help=(
            'edit an arbitrary object, using $EDITOR variable. '
            'In case of a table, only the first 100 rows will be fetched.')
    )
    parser_edit.add_argument(
        'object', help='Object\'s name'
    )
    parser_edit.set_defaults(func=edit_mode)


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
        help=(
            'Copy data to remote server. Can take regular CSV'
            ' directly from STDIN')
    )
    parser_to.add_argument(
        '-f', '--file', help='read from file instead of stdin'
    )
    parser_to.set_defaults(func=raw_mode)


    #  parser for the "copy from" command
    parser_from = subparsers.add_parser(
        'pull',
        help=(
            'Retrieve data from remote server. Outputs regular CSV'
            ' on stdout')
    )
    parser_from.add_argument(
        '-f', '--file', help='write to file instead of stdout'
    )

    parser_from.set_defaults(func=raw_mode)

    parser.add_argument('env', help='environement')

    args = parser.parse_args()
    arguments = vars(args)

    config = read_config(arguments)
    config['current'] = dict(config[arguments['env']])
    config['current'].update(
        {i:arguments[i] for i in arguments if arguments[i]}
    )



    log.debug(str(config['current']))
    validate_config(config)
    connector = connectors.get_connector(config['current'])
    connector.connect()
    if hasattr(args, 'func'):
        args.func(arguments, connector)

parse_args()
