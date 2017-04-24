CREATE OR REPLACE TRIGGER TR_BUI_CASH_VAL_RF_HIST
  BEFORE INSERT OR UPDATE ON cash_val_rf_hist
  FOR EACH ROW
DECLARE
  FUNCTION get_user_name RETURN VARCHAR2 IS
  BEGIN
    RETURN pa_updt_ts.get_user_name;
  END;

BEGIN
  IF pa_updt_ts.update_timestamps() = 1 THEN
    -- Update the timestamps.
    IF (updating) THEN
      IF NOT updating('LAST_UPDT_USER_ID') THEN
        -- No value specified so create a useful one.
        :new.last_updt_user_id := get_user_name;
      END IF;
      IF :new.last_updt_user_id IS NULL THEN
        :new.last_updt_user_id := get_user_name;
      END IF;
    ELSIF (inserting) THEN
      :new.last_updt_user_id := get_user_name; -- Always want this to be the actual user.
    END IF;
    :new.last_updt_ts := SYSDATE; -- Always want the update time (not the supplied time).
  END IF;
END;
