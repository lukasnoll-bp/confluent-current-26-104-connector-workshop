-- ============================================================================
-- IEC 104 Demo – PostgreSQL Initialization
-- The JDBC Sink Connector auto-creates the table (auto.create=true).
-- We install an event trigger so that whenever iec104_measurements is created
-- (by the JDBC sink on first write), inserted_at is added automatically.
-- ============================================================================

-- Function called by the event trigger after every CREATE TABLE
CREATE OR REPLACE FUNCTION add_inserted_at_to_measurements()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
  obj record;
BEGIN
  FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
    IF obj.object_type = 'table'
       AND obj.object_identity = 'public.iec104_measurements' THEN
      ALTER TABLE iec104_measurements
        ADD COLUMN IF NOT EXISTS inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
    END IF;
  END LOOP;
END;
$$;

-- Fire on every CREATE TABLE in this database
CREATE EVENT TRIGGER trg_add_inserted_at
  ON ddl_command_end
  WHEN TAG IN ('CREATE TABLE')
  EXECUTE FUNCTION add_inserted_at_to_measurements();
