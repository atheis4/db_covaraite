-- -------------------------------------------------
-- covariate.model
-- -------------------------------------------------

USE covariate;

DROP TABLE IF EXISTS model;

CREATE TABLE model (
  model_version_id int(11) NOT NULL COMMENT 'fk to covariate.model_version',
  year_id int(11) NOT NULL COMMENT 'fk_to shared.year.year_id. ',
  location_id int(11) NOT NULL COMMENT 'fk to shared.location.location_id',
  sex_id int(11) NOT NULL COMMENT 'fk to shared.sex.sex_id',
  age_group_id int(11) NOT NULL COMMENT 'fk to shared.age_group.age_group_id',
  mean_value double NOT NULL COMMENT 'mean value',
  upper_value double DEFAULT NULL COMMENT 'upper value',
  lower_value double DEFAULT NULL COMMENT 'lower value',
  date_inserted datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'When this row was added to this table. Newly inserted rows default to NOW().',
  inserted_by varchar(50) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL DEFAULT 'unknown' COMMENT 'The user who first added this row',
  last_updated datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'When this row was last edited. Newly inserted rows default to NOW().',
  last_updated_by varchar(50) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL DEFAULT 'unknown' COMMENT 'The user who edited this row last',
  last_updated_action varchar(6) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL DEFAULT 'INSERT' COMMENT 'Either INSERT, UPDATE, DELETE',
  PRIMARY KEY (model_version_id, year_id, location_id, sex_id, age_group_id)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
COMMENT='For GBD Round 6, potential duplicated rows were avoided by limiting the covariate.model table to GBD Round 4 and above. Covariate data from GBD Round 3 and earlier has been archived.'
;

-- Add our foreign key constraints
ALTER TABLE model ADD CONSTRAINT fk_model_model_version_id FOREIGN KEY (model_version_id) REFERENCES model_version (model_version_id);
ALTER TABLE model ADD CONSTRAINT fk_model_location_id FOREIGN KEY (location_id) REFERENCES shared.location (location_id);
ALTER TABLE model ADD CONSTRAINT fk_model_sex_id FOREIGN KEY (sex_id) REFERENCES shared.sex (sex_id);
ALTER TABLE model ADD CONSTRAINT fk_model_year_id FOREIGN KEY (year_id) REFERENCES shared.year (year_id);
ALTER TABLE model ADD CONSTRAINT fk_model_age_group_id FOREIGN KEY (age_group_id) REFERENCES shared.age_group (age_group_id);

-- Create our indices
CREATE INDEX fk_model_location_id ON model (location_id);
CREATE INDEX fk_model_year_id ON model (year_id);
CREATE INDEX fk_model_sex_id ON model (sex_id);
CREATE INDEX fk_model_age_group_id ON model (age_group_id);
