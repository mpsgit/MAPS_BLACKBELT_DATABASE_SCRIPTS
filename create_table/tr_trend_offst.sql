create or replace TRIGGER TR_BUI_TREND_OFFST
--
-- Trigger generated by pa_updt_ts.  Do not change this trigger manually.
--
before insert or update on TREND_OFFST
for each row
--
declare
function get_user_name return varchar2 is
begin
  return pa_updt_ts.get_user_name;
end;

begin
  if pa_updt_ts.update_timestamps() = 1 then        -- Update the timestamps.
    -- "updating('<column>')" works for "updating" but not for "inserting"
    --   so have to compare :new with default value instead (when inserting).
    if (UPDATING) then
      :new.creat_user_id := :old.creat_user_id;     -- Do not want this changed on an update.
      :new.creat_ts      := :old.creat_ts;          -- Do not want this changed on an update.
      if not updating('LAST_UPDT_USER_ID') then     -- No value specified so create a useful one.
        :new.last_updt_user_id := get_user_name;
      end if;
      if :new.last_updt_user_id is null then
        :new.last_updt_user_id := get_user_name;
      end if;
      :new.last_updt_ts := sysdate;                 -- Always want the update time (not the supplied time).
    elsif (INSERTING) then
      -- if not updating('CREAT_USER_ID') then      -- This does not work for inserting, so...
      if :new.creat_user_id = user then             -- Probably not updating this column and ...
        :new.creat_user_id   := get_user_name;      -- Use a more useful value than USER.
      end if;
      if :new.creat_user_id is null then            -- No user supplied.
        :new.creat_user_id   := get_user_name;
      end if;
      :new.creat_ts          := sysdate;            -- Always want the create time (not the supplied time).
      :new.last_updt_user_id := :new.creat_user_id; -- Always want these to be the same on create.
      :new.last_updt_ts      := :new.creat_ts;      -- Always want these to be the same on create.
    end if;
  end if;
end;