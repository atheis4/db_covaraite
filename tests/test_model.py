import pytest
from sqlalchemy.exc import IntegrityError

from db_tools.ezfuncs import get_session
from db_tools.query_tools import exec_query, query_2_df


def initialize_and_fill_covariate_db(covariate_db, cov_conn_def):
    """Builds the covariate db and fills the covariate.model_version table with
    a few rows needed for testing the covariate.model table."""
    covariate_db.add_conn_def(cov_conn_def)
    sesh = get_session(cov_conn_def)

    insert_q = """
        INSERT INTO covariate.model_version (model_version_id, covariate_id, 
            gbd_round_id, description, code_version, status, is_best)
        VALUES ('1', '100', '5', 'pigs per capita', 'version 1', '1', '1'),
               ('2', '728', '4', 'gun violence', 'version 2', '1', '0'),
               ('3', '881', '5', 'sdi', 'version 3', '1', '1');
    """
    exec_query(insert_q, session=sesh)
    sesh.commit()
    # We must close the current session or the next one will hang
    sesh.close()


def test_insert_new_row(covariate_db, cov_conn_def):
    """Tests the addition of a single row to the covariate.model table."""
    initialize_and_fill_covariate_db(covariate_db, cov_conn_def)
    sesh = get_session(cov_conn_def)

    insert_q = """
        INSERT INTO covariate.model (model_version_id, year_id, location_id, 
            sex_id, age_group_id, mean_value, upper_value, lower_value)
        VALUES ('1', '2017', '1', '2', '22', '10.4', '11.2', '9.6'),
               ('1', '2016', '1', '2', '22', '10.5', '11.4', '9.4'),
               ('1', '2015', '1', '2', '22', '10.6', '11.6', '9.2'),
               ('1', '2014', '1', '2', '22', '10.7', '11.8', '9'),
               ('1', '2013', '1', '2', '22', '10.8', '12', '8.8');
    """
    exec_query(insert_q, session=sesh)
    sesh.commit()

    select_q = """
        SELECT * FROM covariate.model WHERE model_version_id = 1;
    """
    res = query_2_df(select_q, session=sesh)

    assert not res.empty
    # We added five entries associated with model_version_id 1
    assert len(res) == 5
    # We have to close the session or the next test will hang
    sesh.close()


def test_pk_integrity_error(covariate_db, cov_conn_def):
    """Test the IntegrityError of our PK by trying to add two rows with the
    same model_version_id, location_id, year_id, sex_id, and age_group_id."""
    initialize_and_fill_covariate_db(covariate_db, cov_conn_def)
    sesh = get_session(cov_conn_def)

    insert_q = """
        INSERT INTO covariate.model (model_version_id, year_id, location_id, 
            sex_id, age_group_id, mean_value, upper_value, lower_value)
        VALUES ('2', '2016', '1', '2', '22', '324', '515', '214');
    """
    exec_query(insert_q, session=sesh)
    sesh.commit()

    with pytest.raises(IntegrityError):
        exec_query(insert_q, session=sesh)
    # We have to close the session or the next test will hang
    sesh.close()


def test_fk_location_id_error(covariate_db, cov_conn_def):
    """Test the foreign key constraint to shared.location by providing an
    invalid location_id to an insert statement."""
    initialize_and_fill_covariate_db(covariate_db, cov_conn_def)
    sesh = get_session(cov_conn_def)

    invalid_location_id = -1

    insert_q = """
        INSERT INTO covariate.model (model_version_id, year_id, location_id, 
            sex_id, age_group_id, mean_value, upper_value, lower_value)
        VALUES ('2', '2016', '{loc_id}', '2', '22', '324', '515', '214');
    """.format(loc_id=invalid_location_id)

    with pytest.raises(IntegrityError):
        exec_query(insert_q, session=sesh)
    # We have to close the session or the next test will hang
    sesh.close()


def test_fk_year_id_error(covariate_db, cov_conn_def):
    """Test the foreign key constraint to shared.year by providing an invalid
    year_id to an insert statement."""
    initialize_and_fill_covariate_db(covariate_db, cov_conn_def)
    sesh = get_session(cov_conn_def)

    invalid_year_id = -1988

    insert_q = """
        INSERT INTO covariate.model (model_version_id, year_id, location_id, 
            sex_id, age_group_id, mean_value, upper_value, lower_value)
        VALUES ('2', '{year_id}', '1', '2', '22', '324', '515', '214');
    """.format(year_id=invalid_year_id)

    with pytest.raises(IntegrityError):
        exec_query(insert_q, session=sesh)
    # We have to close the session or the next test will hang
    sesh.close()


def test_fk_age_group_id_error(covariate_db, cov_conn_def):
    """Test the foreign key constraint to shared.age_group by providing an
    invalid age_group_id to an insert statement."""
    initialize_and_fill_covariate_db(covariate_db, cov_conn_def)
    sesh = get_session(cov_conn_def)

    invalid_age_group_id = -5

    insert_q = """
        INSERT INTO covariate.model (model_version_id, year_id, location_id, 
            sex_id, age_group_id, mean_value, upper_value, lower_value)
        VALUES ('2', '2016', '1', '2', '{age_group_id}', '324', '515', '214');
    """.format(age_group_id=invalid_age_group_id)

    with pytest.raises(IntegrityError):
        exec_query(insert_q, session=sesh)
    # We have to close the session or the next test will hang
    sesh.close()


def test_fk_sex_id_error(covariate_db, cov_conn_def):
    """Test the foreign key constraint to shared.sex by providing an invalid
    sex_id to an insert statement."""
    initialize_and_fill_covariate_db(covariate_db, cov_conn_def)
    sesh = get_session(cov_conn_def)

    invalid_sex_id = -8

    insert_q = """
        INSERT INTO covariate.model (model_version_id, year_id, location_id, 
            sex_id, age_group_id, mean_value, upper_value, lower_value)
        VALUES ('2', '2016', '1', '{sex_id}', '22', '324', '515', '214');
    """.format(sex_id=invalid_sex_id)

    with pytest.raises(IntegrityError):
        exec_query(insert_q, session=sesh)
    # We have to close the session or the next test will hang
    sesh.close()


def test_fk_model_version_error(covariate_db, cov_conn_def):
    """Test the foreign key constraint to covariate.model_version by providing
    an invalid model_version_id to an insert statement."""
    initialize_and_fill_covariate_db(covariate_db, cov_conn_def)
    sesh = get_session(cov_conn_def)

    invalid_model_version_id = 5

    insert_q = """
        INSERT INTO covariate.model (model_version_id, year_id, location_id, 
            sex_id, age_group_id, mean_value, upper_value, lower_value)
        VALUES ('{mvid}', '2016', '1', '2', '22', '324', '515', '214');
    """.format(mvid=invalid_model_version_id)

    with pytest.raises(IntegrityError):
        exec_query(insert_q, session=sesh)
    # We have to close the session or the next test will hang
    sesh.close()
