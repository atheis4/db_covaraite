db_covariate
===============================================================================
- Author: Andrew Theis
- Email: atheis@uw.edu
- Description: A deployable version of the 2019 GBD covariate database.

**about**
===============================================================================
db_covariate represents the first of the Global Burden of Disease databases to be turned into a deployable python package.

This will allow any test suites that rely on querying or updating data from this database to divorce itself from IHME infrastructure.

The database is created using an internal mock database called EphemerDB. Developed by Scientific Computing at IHME, EphemerDB creates a Docker or Singularity container with a mysql instance.

A similar version of the Shared Database must be created first so that the foreign keys to the covariate database are not violated.


**use**
===============================================================================
Please check the tests/conftest.py module for an example of creating the database.

You must have docker, mysql, and the proprietary cluster_utils installed on your local machine in order to create the containerized database.
