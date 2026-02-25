-- Migration: Add LISTEN/NOTIFY trigger for completed network scans
-- Apply to Stellarbeat's PostgreSQL database
-- This trigger fires once per scan completion (~every 5 minutes)

CREATE OR REPLACE FUNCTION notify_scan_completed()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.completed = true AND (OLD.completed = false OR OLD.completed IS NULL) THEN
        PERFORM pg_notify('scan_completed', NEW.id::text);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER scan_completed_trigger
AFTER UPDATE ON network_scan
FOR EACH ROW EXECUTE FUNCTION notify_scan_completed();
