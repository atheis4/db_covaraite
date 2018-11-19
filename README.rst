db_covariate
===============================================================================
- Author: Andrew Theis
- Email: atheis@uw.edu
- Description: A deployable version of the 2019 GBD covariate database.

**ABOUT**

db_covariate represents the first of the Global Burden of Disease databases to be turned into a deployable python package.

This will allow any test suites that rely on querying or updating data from this database to divorce itself from IHME infrastructure.

The database is created using an internal mock database called EphemerDB. Developed by Scientific Computing at IHME, EphemerDB creates a Docker or Singularity container with a mysql instance.

A similar version of the Shared Database must be created first so that the foreign keys to the covariate database are not violated.

The make_schema module contains the CovariateDB object that must be instantiated with the host, user, password of the mysql Docker database.