import os
import re
from subprocess import check_output
import sys

from db_tools import config


this_path = os.path.dirname(os.path.abspath(__file__))
this_dir = os.path.dirname(__file__)


class CovariateDB(object):

    tables = ['model_version.sql',
              'model.sql']

    triggers = []
    sprocs = []

    def __init__(self, host, port, user, password, root_conn_str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.root_conn_str = root_conn_str

        # The following lines are necessary for exposing the newly created
        # database to our pattern of database access through
        # db_tools.ezfuncs.get_session which returns a sqlalchemy.orm.Session
        # object.
        params = re.match('.*://(.*):(.*)@(.*):(.*)/(.*)',
                          root_conn_str)

        new_conn_def = {
            'covariates': {
                'host': params.group(3),
                'port': params.group(4),
                'user_name': params.group(1),
                'password': params.group(2),
                'default_schema': params.group(5),
                'pool_recycle': 360
            }
        }

        cfg = config.DBConfig(
            load_base_defs=True,
            base_filepath=os.path.join(this_path, '.odbc.ini')
        )

        cfg.add_conn_defs(new_conn_def)

    def execute_sql_script(self, sql_script):
        cmd = ('mysql -h {host} -P {port} --user={user} --password={password} '
               '< {sql}'.format(host=self.host,
                                port=self.port,
                                user=self.user,
                                password=self.password,
                                sql=sql_script))
        stdout = check_output(cmd, shell=True)
        return stdout

    def build_db(self):
        # Create the schema
        self.execute_sql_script(os.path.join(this_dir, 'make_schema.sql'))

        # Build the tables in order
        for table in self.tables:
            table_sql_script = os.path.join(this_dir, 'building_blocks', table)
            self.execute_sql_script(table_sql_script)
