import shlex
import sys
from typing import Any
import cmd2
from cmd2 import Cmd2ArgumentParser, with_argparser


# This is a hand-rolled pysqlite3 from https://github.com/coleifer/pysqlite3
# The setup.py is edited to make the list of compilation options mutually consistent
# wrt https://github.com/nalgeon/sqlite#sqlite-shell-builder as I like to
# prototype features using the sqlite3 *shell* and then only 'fall up' to Python when there
# is something 'fancy' to be done.
import pysqlite3

import rich
from rich.table import Table
from rich import box
from rich.console import Console

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Session=sessionmaker()

import textual
from argopt import argopt

def hasdocopt(f, *args, **kwargs):
    parser=argopt(f.__doc__)
    return with_argparser(parser)(f)
    
class RichCmd(cmd2.Cmd):
    def __init__(self):
        super().__init__()
        self.console=Console()
        # Make maxrepeats settable at runtime
    
    def poutput(self, msg: Any='',*,end:str="\n") -> None:
        self.console.print(msg)
        # with self.console.capture() as capture:
        #     self.console.print(msg)
        # str_output=capture.get()
        # super().poutput(str_output,end=end)

    def perror(self, msg: Any = '', *, end: str = '\n', apply_style: bool = True) -> None:
        self.console.print(msg)
        # with self.console.capture() as capture:
        #     self.console.print(msg)
        # str_output=capture.get()
        # super().perror(str_output,end=end,apply_style=apply_style)

    def pexcept(self, msg: Any, *, end: str = '\n', apply_style: bool = True) -> None:
        #self.console.print(msg)
        if self.debug and sys.exc_info() != (None,None,None):
            self.console.print_exception(show_locals=True)
        else:
            super().pexcept(msg,end=end,apply_style=apply_style)


class MechE(RichCmd):
    prompt="MechE$ "
    def __init__(self):
        super().__init__()
        self.rule4 = "sqlite://"
        self.add_settable(cmd2.Settable('rule4', str, 'SQLAlchemy URI for Rule4 database', self))
        self.engine=create_engine(self.rule4)
        self.register_precmd_hook(self.setup_session)
        self.register_postcmd_hook(self.close_session)

    def setup_session(self, data: cmd2.plugin.PrecommandData) -> cmd2.plugin.PrecommandData:
        self.S = Session(bind=self.engine)
        return data

    def close_session(self, data: cmd2.plugin.PostcommandData) -> cmd2.plugin.PostcommandData:
        try:
            self.S.close()
        except AttributeError:
            pass
        return data

    def _onchange_rule4(self, param_name, old, new):
        try:
            self.engine.close()
        except:
            pass
        mod=None
        if new.startswith('sqlite'):
            mod=pysqlite3
        self.engine = create_engine(new, module=mod)  

    @hasdocopt
    def do_db(self,opts):
        '''db

Usage:
    db
    '''
        c = MechEDb()
        c.cmdloop()

    @hasdocopt
    def do_banana(self, opts):
        '''Example programme description.
You should be able to do
    args = argopt(__doc__).parse_args()
instead of
    args = docopt(__doc__)

Usage:
    banana [options] <x> [<y>...]

Arguments:
    <x>                   A file.
    --anarg=<a>           Description here [default: 1e3:int].
    -p PAT, --patts PAT   Or [default: None:file].
    --bar=<b>             Another [default: something] should
                          auto-wrap something in quotes and assume str.
    -f, --force           Force.
'''
        t = Table("anarg", "patts", "fruit", title=opts.bar, box=box.MINIMAL_DOUBLE_HEAD)
        self.console.print(t)


class MechEDb(MechE):
    prompt="db> "
    
    @hasdocopt
    def do_compile(self, opts):
        '''compile
        
Usage: compile [options]    

Arguments:
  -v, --verbose     Verbose
        '''
        t = Table("option", title="Compile Options", box=box.MINIMAL_DOUBLE_HEAD)
        r = self.S.execute("PRAGMA compile_options;").fetchall()
        for row in r:
            t.add_row(*row)
        self.console.print(t)

    @hasdocopt
    def do_modules(self, opts):
        '''modules
        
Usage: modules [options]    

Arguments:
  -v, --verbose     Verbose
        '''
        t = Table("module", title="Modules", box=box.MINIMAL_DOUBLE_HEAD)
        r = self.S.execute("SELECT name from pragma_module_list;").fetchall()
        for row in r:
            t.add_row(*row)
        self.console.print(t)

    @hasdocopt
    def do_query(self, opts):
        '''query
        
Usage: query <q>

Arguments:
  -v, --verbose     Verbose
  <q>               Query text
        '''
        self.console.print(opts)
app = MechE()
app.cmdloop()