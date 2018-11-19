-- This script represents the ETL of a single model_version's data from the
-- model table to its new schema for GBD 2019.

INSERT INTO model_new
  (model_version_id,
   year_id,
   location_id,
   sex_id,
   age_group_id,
   mean_value,
   upper_value,
   lower_value,
   date_inserted,
   inserted_by,
   last_updated,
   last_updated_by,
   last_updated_action)
SELECT m.model_version_id,
       m.year_id,
       m.location_id,
       m.sex_id,
       m.age_group_id,
       m.mean_value,
       m.upper_value,
       m.lower_value,
       m.date_inserted,
       m.inserted_by,
       m.last_updated,
       m.last_updated_by,
       m.last_updated_action
FROM model m
WHERE model_verion_id = :model_version_id
;