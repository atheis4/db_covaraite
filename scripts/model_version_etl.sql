-- --------------------------------------------------
-- covariate.model_version_new
-- --------------------------------------------------

USE covariate;--

DROP TABLE IF EXISTS model_version_new;--

CREATE TABLE model_version_new (
  model_version_id int(11) NOT NULL AUTO_INCREMENT COMMENT 'meta-information for model',
  covariate_id int(11) NOT NULL COMMENT 'FK to shared.covariate.covariate_id',
  gbd_round_id int(11) NOT NULL COMMENT 'FK to shared.gbd_round.gbd_round_id',
  description varchar(255) COLLATE utf8_unicode_ci NOT NULL COMMENT 'Description of model run',
  code_version varchar(500) DEFAULT NULL COMMENT 'The source code repository information. Provide the ability to recreate the code used to make this model_version',
  status tinyint(4) NOT NULL COMMENT 'status indicator of model run (0=running, 1=complete, 2=submitted, 3=deleted)',
  is_best tinyint(4) NOT NULL COMMENT 'best model indicator (0=not best, 1=best, 2=was previous best)',
  best_start datetime DEFAULT NULL COMMENT 'date best model marked',
  best_end datetime DEFAULT NULL COMMENT 'date best model unmarked',
  best_user varchar(128) COLLATE utf8_unicode_ci DEFAULT NULL COMMENT 'who marked it',
  best_description varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL COMMENT 'description of why best model marked',
  date_inserted datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'When this row was added to this table. Newly inserted rows default to NOW().  ',
  inserted_by varchar(50) COLLATE utf8_unicode_ci NOT NULL DEFAULT 'unknown' COMMENT 'The user who first added this row',
  last_updated datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'When this row was last edited. Newly inserted rows default to NOW().  ',
  last_updated_by varchar(50) COLLATE utf8_unicode_ci NOT NULL DEFAULT 'unknown' COMMENT 'The user who edited this row last',
  last_updated_action varchar(6) COLLATE utf8_unicode_ci NOT NULL DEFAULT 'INSERT' COMMENT 'Either INSERT, UPDATE, DELETE',
  PRIMARY KEY (model_version_id)
) ENGINE=InnoDB AUTO_INCREMENT=24338 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- CONSTRAINTS --
ALTER TABLE model_version ADD CONSTRAINT fk_model_version_covariate_id FOREIGN KEY (covariate_id) REFERENCES shared.covariate (covariate_id);--
ALTER TABLE model_version ADD CONSTRAINT fk_model_version_gbd_round_id FOREIGN KEY (gbd_round_id) REFERENCES shared.gbd_round (gbd_round_id);--
ALTER TABLE model_version ADD CONSTRAINT covariate_id_gbd_round_id_is_best UNIQUE (covariate_id, gbd_round_id, is_best);--

-- INDICES --
CREATE INDEX fk_model_version_covariate_id ON model_version_new (covariate_id);--
CREATE INDEX fk_model_version_gbd_round_id ON model_version_new (gbd_round_id);--
CREATE INDEX fk_model_version_covariate_gbd_round_ids ON model_version_new (covariate_id, gbd_round_id);--
