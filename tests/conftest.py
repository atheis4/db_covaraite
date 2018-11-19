import pytest
import socket

from cluster_utils.ephemerdb import create_ephemerdb
from db_shared.make_schema import SharedUp

from db_covariate.make_schema import CovariateDB


@pytest.fixture(scope='session')
def server_instance():
    server = create_ephemerdb('5.7')
    server.start()
    yield server
    server.stop()


@pytest.fixture(scope='session')
def shared_db(server_instance):
    shared = SharedUp(host=socket.gethostname(),
                      port=server_instance.db_port,
                      user="root",
                      password=server_instance.db_root_pass)
    shared.build_db()
    yield


@pytest.fixture(scope='session')
def cov_conn_def():
    return 'cov'


@pytest.fixture(scope='function')
def covariate_db(shared_db, server_instance):
    """Creates the covariate database in our existing mysql docker container
    holding the shared database.

    At the beginning of each test function, this fixture should be called to
    instantiate a new instance of the covariate database. When a new instance
    is called, all existing tables and rows for model and model_version will be
    dropped.

    Arguments (required fixtures):
        shared_db (db_shared.make_schema.SharedUp): this is a fixture that
            created our shared database and holds the credentials that we need
            to build additional tables that rely on shared data.
        server_instance (cluster_utils.ephemerdb.EphemerDB)

    Returns:
        db_covariate.make_schema.CovaraiteDB object.
    """
    cov_db = CovariateDB(host=socket.gethostname(),
                         port=server_instance.db_port,
                         user="root",
                         password=server_instance.db_root_pass,
                         root_conn_str=server_instance.root_conn_str)
    cov_db.build_db()
    return cov_db
