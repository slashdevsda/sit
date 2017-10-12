import pymssql


##
## GUI
##


from prompt_toolkit.contrib.completers import WordCompleter

from prompt_toolkit import prompt
from prompt_toolkit.keys import Keys
from pygments.lexers import SqlLexer
from prompt_toolkit.history import InMemoryHistory

from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import Completer, Completion
class SQLDynamicCompleter(Completer):
    def get_completions(self, document, complete_event):
        '''
        Dynamically compute a completion list object
        (prompt_toolkit.completion.Completion)
        '''
        completions = [
            'SELECT',
            'FROM',
            'WHERE',
            'RIGHT OUTER JOIN',
            'LEFT OUTER JOIN',
            'LEFT JOIN',
            'RIGHT JOIN',
            'INNER JOIN',
            'DROP',
            'CREATE',
            'UPDATE',
        ]

        st = document.find_previous_word_beginning()

        for c in completions:
            yield Completion(c, start_position=st if st else 0)




class Shell:
    def __init__(self, connector):


        # Create key bindings registry with a custom binding for the Tab key that
        # displays completions like GNU readline.
        #registry = load_key_bindings()
        #registry.add_binding(Keys.ControlI)(display_completions_like_readline)
        self.connector = connector
        self.history = InMemoryHistory()
        self.completer = SQLDynamicCompleter()

    def prompt_query(self):
            return prompt(
                '→ ',
                completer=self.completer,
                lexer=SqlLexer,
                history=self.history,
                auto_suggest=AutoSuggestFromHistory(),
                complete_while_typing=False
            )

    def loop(self):
        while True:
            text = self.prompt_query()
            print('➪ %s' % text)
            try:
                print(self.connector.exec_query(text))
                self.connector.commit_transaction()
            except Exception as e:
                print('error: {}'.format(str(e)))
                self.connector.rollback_transaction()
