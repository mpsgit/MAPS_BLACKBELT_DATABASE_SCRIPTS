CREATE OR REPLACE TRIGGER TR_BUI_CASH_VAL_RF_HIST 
BEFORE INSERT OR UPDATE ON CASH_VAL_RF_HIST for each row
declare
function get_user_name return varchar2 is
begin
  return pa_updt_ts.get_user_name;
end;

begin
  if pa_updt_ts.update_timestamps() = 1 then        -- Update the timestamps.
    if (UPDATING) then
      if not updating('LAST_UPDT_USER_ID') then     -- No value specified so create a useful one.
        :new.last_updt_user_id := get_user_name;
      end if;
      if :new.last_updt_user_id is null then
        :new.last_updt_user_id := get_user_name;
      end if;
    elsif (INSERTING) then
      :new.last_updt_user_id := get_user_name;    -- Always want this to be the actual user.
    end if;
    :new.last_updt_ts := sysdate;                 -- Always want the update time (not the supplied time).
  end if;
end;