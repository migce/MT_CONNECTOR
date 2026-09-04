\set ON_ERROR_STOP on
-- TimescaleDB 2.25.2. Run through psql, not inside an API transaction.
-- Keeps the original policy/config for inspection and straightforward rollback.
BEGIN;
SET LOCAL lock_timeout='2s';
SET LOCAL statement_timeout='10s';
CREATE OR REPLACE PROCEDURE public.connector_guarded_compression(job_id integer, config jsonb)
LANGUAGE plpgsql AS $$
DECLARE
  original_id integer := (config->>'original_job_id')::integer;
  policy_config jsonb;
  previous_lock_timeout text := current_setting('lock_timeout');
BEGIN
  SELECT j.config INTO STRICT policy_config
  FROM timescaledb_information.jobs j
  WHERE j.job_id=original_id AND j.proc_schema='_timescaledb_functions'
    AND j.proc_name='policy_compression' AND NOT j.scheduled;
  -- Session lock deliberately survives native policy's per-chunk COMMITs.
  -- On failure the dedicated background-job connection exits and releases it.
  IF NOT pg_try_advisory_lock(788601100001::bigint) THEN
    RAISE EXCEPTION 'Interactive history is busy; compression deferred' USING ERRCODE='55P03';
  END IF;
  PERFORM set_config('lock_timeout','1s',false);
  CALL _timescaledb_functions.policy_compression(original_id,policy_config);
  PERFORM set_config('lock_timeout',previous_lock_timeout,false);
  PERFORM pg_advisory_unlock(788601100001::bigint);
END;
$$;
COMMENT ON PROCEDURE public.connector_guarded_compression(integer,jsonb) IS
  'Dedicated Timescale job only: shared fail-fast history gate, 1s lock wait, native per-chunk commits. Close connection after a failed manual CALL.';
DO $$
DECLARE original_job record; guarded_id integer;
BEGIN
  SELECT * INTO STRICT original_job FROM timescaledb_information.jobs
  WHERE hypertable_schema='public' AND hypertable_name='ticks'
    AND proc_schema='_timescaledb_functions' AND proc_name='policy_compression';
  SELECT job_id INTO guarded_id FROM timescaledb_information.jobs
  WHERE proc_schema='public' AND proc_name='connector_guarded_compression';
  IF guarded_id IS NULL THEN
    guarded_id := add_job('public.connector_guarded_compression', original_job.schedule_interval,
      config=>jsonb_build_object('original_job_id',original_job.job_id),scheduled=>false);
  END IF;
  PERFORM alter_job(original_job.job_id,scheduled=>false,max_runtime=>interval '5 minutes');
  PERFORM alter_job(guarded_id,scheduled=>true,max_runtime=>interval '5 minutes',
    retry_period=>interval '5 minutes',max_retries=>-1);
END;
$$;
COMMIT;
SELECT job_id,proc_name,scheduled,max_runtime,retry_period,config
FROM timescaledb_information.jobs
WHERE proc_name IN ('policy_compression','connector_guarded_compression');
