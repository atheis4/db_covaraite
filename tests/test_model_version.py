import pytest
from sqlalchemy.exc import IntegrityError

from db_tools.ezfuncs import get_session
from db_tools.query_tools import exec_query, query_2_df


def test_insert_new_row(covariate_db, cov_conn_def):
    """Inserts a new row into the model_version table and queries it to make
    sure that the model version ids match."""
    covariate_db.add_conn_def(cov_conn_def)
    sesh = get_session(cov_conn_def)

    dummy_mvid = 1
    insert_q = """
        INSERT INTO covariate.model_version (model_version_id, covariate_id, 
            description, code_version, status, is_best, gbd_round_id)
        VALUES ('{mvid}', '1', 'best dummy round 5', 'vers 1', '1', '0', '5');
    """.format(mvid=dummy_mvid)
    exec_query(insert_q, session=sesh)
    sesh.commit()

    select_q = """
        SELECT * FROM covariate.model_version WHERE model_version_id = {mvid};
    """.format(mvid=dummy_mvid)

    res = query_2_df(select_q, session=sesh)
    assert not res.empty
    assert res.at[0, 'model_version_id'] == dummy_mvid
    # we need to close the session or the next test will hang
    sesh.close()


def test_pk_integrity_error(covariate_db, cov_conn_def):
    """Inserts a new row and then attempts to trigger an IntegrityError by
    inserting the same data a second time."""
    covariate_db.add_conn_def(cov_conn_def)
    sesh = get_session(cov_conn_def)

    dummy_mvid = 1
    insert_q = """
        INSERT INTO covariate.model_version (model_version_id, covariate_id,
            description, code_version, status, is_best, gbd_round_id)
        VALUES ('{mvid}', '1', 'testing PK error', 'vers 1', '1', '0', '5');
    """.format(mvid=dummy_mvid)
    exec_query(insert_q, session=sesh)
    sesh.commit()

    with pytest.raises(IntegrityError):
        exec_query(insert_q, session=sesh)
    # we need to close the session or the next test will hang
    sesh.close()


def test_fk_covariate_id_error(covariate_db, cov_conn_def):
    """Tries to insert a bad covariate_id into the model_version table."""
    covariate_db.add_conn_def(cov_conn_def)
    sesh = get_session(cov_conn_def)

    invalid_covariate_id = -1
    insert_q = """
        INSERT INTO covariate.model_version (covariate_id, description, 
            code_version, status, is_best, gbd_round_id)
        VALUES ('{cov_id}', 'trigger bad FK', 'version 1', '1', '0', '5');
    """.format(cov_id=invalid_covariate_id)

    with pytest.raises(IntegrityError):
        exec_query(insert_q, session=sesh)
    # we need to close the session or the next test will hang
    sesh.close()


def test_fk_gbd_round_id_error(covariate_db, cov_conn_def):
    """Tries to insert a bad gbd_round_id into the model_version table."""
    covariate_db.add_conn_def(cov_conn_def)
    sesh = get_session(cov_conn_def)

    invalid_gbd_round_id = -1
    insert_q = """
        INSERT INTO covariate.model_version (covariate_id, description, 
            code_version, status, is_best, gbd_round_id)
        VALUES ('1', 'trigger bad FK', 'vers 1', '1', '0', '{gbd_round_id}');
    """.format(gbd_round_id=invalid_gbd_round_id)

    with pytest.raises(IntegrityError):
        exec_query(insert_q, session=sesh)
    # we need to close the session or the next test will hang
    sesh.close()


def test_unique_covariate_round_best_constraint(covariate_db, cov_conn_def):
    """Tries to insert two models marked best for the same covariate for a
    single gbd_round_id."""
    covariate_db.add_conn_def(cov_conn_def)
    sesh = get_session(cov_conn_def)

    insert_q = """
        INSERT INTO covariate.model_version (covariate_id, description,
            code_version, status, is_best, gbd_round_id)
        VALUES ('881', 'best SDI marked best', 'version 1', '1', '1', '5');
    """
    exec_query(insert_q, session=sesh)
    sesh.commit()

    # Insert the same composite of covariate_id, gbd_round_id, and is_best to
    # trigger failure
    with pytest.raises(IntegrityError):
        exec_query(insert_q, session=sesh)
    # we need to close the session or the next test will hang
    sesh.close()


def test_insert_defaults(covariate_db, cov_conn_def):
    """Insert a new row into the covariate.model_version without a best_start,
    best_end, best_user, best_description column to check the default value."""
    covariate_db.add_conn_def(cov_conn_def)
    sesh = get_session(cov_conn_def)

    insert_q = """
        INSERT INTO covariate.model_version (model_version_id, covariate_id, 
            description, code_version, status, is_best, gbd_round_id)
        VALUES ('1', '1', 'testing defaults', 'version 1', '1', '0', '5');
    """
    exec_query(insert_q, session=sesh)
    sesh.commit()

    select_q = """
        SELECT * FROM covariate.model_version WHERE model_version_id = 1;
    """
    res = query_2_df(select_q, session=sesh)

    assert not res.empty
    assert not res.at[0, 'best_user']
    assert not res.at[0, 'best_description']
    assert not res.at[0, 'best_start']
    assert not res.at[0, 'best_end']
    # we need to close the session or the next test will hang
    sesh.close()
