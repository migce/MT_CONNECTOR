\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout='2s';
SET LOCAL statement_timeout='10s';
DO $$
DECLARE guarded record;
BEGIN
  FOR guarded IN SELECT job_id,config FROM timescaledb_information.jobs
    WHERE proc_schema='public' AND proc_name='connector_guarded_compression'
  LOOP
    PERFORM alter_job((guarded.config->>'original_job_id')::integer,scheduled=>true);
    PERFORM delete_job(guarded.job_id);
  END LOOP;
END;
$$;
DROP PROCEDURE IF EXISTS public.connector_guarded_compression(integer,jsonb);
COMMIT;
