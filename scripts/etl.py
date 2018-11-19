import argparse
import os

from db_tools.query_tools import exec_query, query_2_df
from db_tools.ezfuncs import get_session


TABLE_CREATE_SCRIPTS = ['model_version_etl.sql', 'model_etl.sql']
TABLE_COPY_SCRIPTS = {
    'model_version': 'model_version_copy.sql',
    'model': 'model_copy.sql'
}


def create_schema(sesh, root_dir):
    """Runs the make_schema.sql script on our covariate database. This needs to
    be run before tables can be created on test or prod.

    Arguments:
        sesh (sqlalchemy.orm.session.Session): our connection to the covariate
            database.
        root_dir (str, path): regardless of database server, our make_schema
            script is the same. It has a fixed path from root_dir so let's
            supply that and construct the absolute path to the script.
    """
    sql_script = os.path.join(root_dir, 'db_covariate/make_schema.sql')
    sql_lines = parse_sql_script(sql_script)

    for line in sql_lines:
        exec_query(line, session=sesh)
        sesh.commit()


def parse_sql_script(script):
    """
    Accepts a path to a sql script and parses the script to convert the
    individual commands into elements in a list.

    Requires the end of each SQL command to have '--' characters (SQL's inline
    comment). This is necessary for identifying multi-line SQL commands and
    parsing them appropriately.

    Arguments:
        script (str, path): the absolute path to the SQL script.

    Returns:
        a list of individual SQL commands contained in the script.
    """
    with open(script, 'r') as f:
        raw_sql = f.read()

    sql_lines = [x for x in raw_sql.replace('\n', '').split('--') if ';' in x]
    return sql_lines


def parse_args():
    """Accept cmd line arguments to determine either test or prod servers."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--prod', type=str, default='test')

    args = parser.parse_args()
    return args.prod


def get_model_version_ids(sesh):
    """Return a list of model_version_ids.

    Arguments:
        sesh (sqlalchemy.orm.session.Session): connection to our database
            server.
    """
    q = """
        SELECT model_version_id
        FROM covariate.model_version_new
        ORDER BY model_version_id
    """
    res = query_2_df(q, session=sesh)
    return res.model_version_id.tolist()


def write_success_file(code_dir, completed_mvids):
    """
    Save a log of the model_version_ids successfully ETL'ed to the new table.

    Arguments:
        code_dir (str, path): our code directory
        completed_mvids (intlist): A list of successfully copied
            model_version_ids
    """
    with open(os.path.join(code_dir, 'complete.txt'), 'w') as f:
        f.write('model_versions completed:')
        for mvid in completed_mvids:
            f.write("{}".format(mvid))


def main():
    """The main function is responsible for running the full ETL pipeline from
    front to back.

    First we will create the variables needed to generate our tables: including
    the directories, file names, and database server we'll point to.

    Next we will create the database schema and build the empty tables. The
    empty tables will have '_new' attached to the end of them. This will allow
    us to copy data over from the existing table before dropping the existing
    table and renaming the '_new' table with the original table name.

    Finally we will ETL the data. The model_version table is small enough that
    we can just copy the entire table (except for models from GBD Round 3 and
    earlier).

    For the model table, we will iterate over the individual model_version_ids
    and copy all the data from the model table one model_version at a time.
    """
    code_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

    environment = parse_args()

    if environment == 'prod':
        sesh = get_session('covariate')
    else:
        sesh = get_session('covariate-test')

    # Create our tables
    for table in TABLE_CREATE_SCRIPTS:
        sql_script = os.path.join(code_dir, table)
        # Parse into a list of individual commands
        sql_lines = parse_sql_script(sql_script)

        # Run those commands one at a time and commit the session.
        for line in sql_lines:
            exec_query(line, session=sesh)
            sesh.commit()

    # ETL the model_version table.
    # Here we need to make a copy of all rows (with a slight modification to
    # swap data_version_id with covariate_id) from the existing model_version
    # table that are associated with GBD round greater than round 3.
    mv_etl_script = os.path.join(code_dir, TABLE_COPY_SCRIPTS['model_version'])
    sql_lines = parse_sql_script(mv_etl_script)

    for line in sql_lines:
        exec_query(line, session=sesh)
        sesh.commit()

    # ETL the model table.
    # This is a little trickier. There are hundreds of millions of rows we need
    # to copy. So we are going to read the model_version_ids from the newly
    # created model_version_new table and then iterate over these
    # model_version_ids to process one at a time. We'll keep track of which
    # ones have successfully finished so we'll have a record of what has
    # already been copied in the event of failure midway.
    completed = []
    failed = []
    # Return a sorted list of model_version_ids from the newly etl'ed table.
    model_version_ids = get_model_version_ids(sesh)

    model_etl_script = os.path.join(code_dir, TABLE_COPY_SCRIPTS['model'])
    sql_lines = parse_sql_script(model_etl_script)

    for mvid in model_version_ids:
        for line in sql_lines:
            # This might fail if we haven't specified USE covariate;...
            try:
                sesh.execute(line, params={'model_version_id': mvid})
                sesh.commit()
                completed.append(mvid)
            except Exception as e:  # I'm unsure what the exception could be
                failed.append(mvid)
                print(e)
                print(failed)

    # In the event of a failure, or success! Let's print out a list of the
    # model versions that succeeded so we can start from where we left off.
    if len(failed):
        write_success_file(code_dir, completed)


if __name__ == '__main__':
    main()
