﻿CREATE OR REPLACE TRIGGER tr_bui_custm_rul_skulst
--
  -- Trigger generated by pa_updt_ts.  Do not change this trigger manually.
  --
  BEFORE INSERT OR UPDATE ON custm_rul_sku_list
  FOR EACH ROW
--
DECLARE
  FUNCTION get_user_name RETURN VARCHAR2 IS
  BEGIN
    RETURN pa_updt_ts.get_user_name;
  END;
BEGIN
  IF pa_updt_ts.update_timestamps() = 1 THEN
    -- Update the timestamps.
    -- "updating('<column>')" works for "updating" but not for "inserting"
    --   so have to compare :new with default value instead (when inserting).
    IF (updating) THEN
      :new.creat_user_id := :old.creat_user_id; -- Do not want this changed on an update.
      :new.creat_ts      := :old.creat_ts; -- Do not want this changed on an update.
      IF NOT updating('LAST_UPDT_USER_ID') THEN
        -- No value specified so create a useful one.
        :new.last_updt_user_id := get_user_name;
      END IF;
      IF :new.last_updt_user_id IS NULL THEN
        :new.last_updt_user_id := get_user_name;
      END IF;
      :new.last_updt_ts := SYSDATE; -- Always want the update time (not the supplied time).
    ELSIF (inserting) THEN
      -- if not updating('CREAT_USER_ID') then      
      -- This does not work for inserting, so...
      IF :new.creat_user_id = USER THEN
        -- Probably not updating this column and ...
        :new.creat_user_id := get_user_name; -- Use a more useful value than USER.
      END IF;
      IF :new.creat_user_id IS NULL THEN
        -- No user supplied.
        :new.creat_user_id := get_user_name;
      END IF;
      :new.creat_ts          := SYSDATE; -- Always want the create time (not the supplied time).
      :new.last_updt_user_id := :new.creat_user_id; -- Always want these to be the same on create.
      :new.last_updt_ts      := :new.creat_ts; -- Always want these to be the same on create.
    END IF;
  END IF;
END;
