-- This script represents the full ETL of the model_version table to its new
-- schema for GBD 2019.

INSERT INTO model_version_new
  (model_version_id,
   covariate_id,
   gbd_round_id,
   description,
   code_version,
   status,
   is_best,
   best_start,
   best_end,
   best_user,
   best_description,
   date_inserted,
   inserted_by,
   last_updated,
   last_updated_by,
   last_updated_action)
SELECT mv.model_version_id,
       dv.covariate_id,
       mv.gbd_round_id,
       mv.description,
       mv.code_version,
       mv.status,
       mv.is_best,
       mv.best_start,
       mv.best_end,
       mv.best_user,
       mv.best_description,
       mv.date_inserted,
       mv.inserted_by,
       mv.last_updated,
       mv.last_updated_by,
       mv.last_updated_action
FROM model_version mv
JOIN data_version dv USING (data_version_id)
WHERE mv.gbd_round_id > 3;--