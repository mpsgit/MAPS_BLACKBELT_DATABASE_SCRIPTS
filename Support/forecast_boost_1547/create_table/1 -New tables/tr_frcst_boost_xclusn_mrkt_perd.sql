CREATE OR REPLACE TRIGGER tr_bui_frcst_bst_xclsn_mrktprd
--
  BEFORE INSERT OR UPDATE ON frcst_boost_xclusn_mrkt_perd
  FOR EACH ROW
--
DECLARE
  v_cnt PLS_INTEGER;
  FUNCTION get_user_name RETURN VARCHAR2 IS
  BEGIN
    RETURN pa_updt_ts.get_user_name;
  END;
BEGIN
  IF pa_updt_ts.update_timestamps() = 1 THEN
    -- Update the timestamps.
    -- "updating('<column>')" works for "updating" but not for "inserting"
    --   so have to compare :new with default value instead (when inserting).
    -- check Categories
    IF :new.catgry_id_list IS NOT NULL THEN
      FOR i IN (SELECT TRIM(regexp_substr(col, '[^,]+', 1, LEVEL)) catgry_id
                  FROM (SELECT :new.catgry_id_list AS col FROM dual)
                CONNECT BY LEVEL <= length(regexp_replace(col, '[^,]+')) + 1) LOOP
        -- check if i.CATGRY_ID is NUMBER
        IF TRIM(REPLACE(translate(i.catgry_id, '0123456789', '0000000000'),
                        '0',
                        '')) IS NOT NULL THEN
          raise_application_error(-20001,
                                  'catgry_id_list ''' ||
                                  :new.catgry_id_list ||
                                  ''' MUST contain only NUMERIC character(s)');
        END IF;
        -- check if i.CATGRY_ID is VALID in CATGRY
        SELECT COUNT(1)
          INTO v_cnt
          FROM catgry
         WHERE catgry_id = i.catgry_id
           AND dltd_ind = 'N';
        IF v_cnt <> 1 THEN
          raise_application_error(-20001,
                                  'validation of element(' || i.catgry_id ||
                                  ') from catgry_id_list ''' ||
                                  :new.catgry_id_list ||
                                  ''' FAILED in CATGRY table');
        END IF;
      END LOOP;
    END IF;
    -- check Sales Classes
    IF :new.sls_cls_cd_list IS NOT NULL THEN
      FOR i IN (SELECT TRIM(regexp_substr(col, '[^,]+', 1, LEVEL)) sls_cls_cd
                  FROM (SELECT :new.sls_cls_cd_list AS col FROM dual)
                CONNECT BY LEVEL <= length(regexp_replace(col, '[^,]+')) + 1) LOOP
        -- check if i.SLS_CLS_CD is VALID in SLS_CLS
        SELECT COUNT(1)
          INTO v_cnt
          FROM sls_cls
         WHERE sls_cls_cd = i.sls_cls_cd;
        IF v_cnt <> 1 THEN
          raise_application_error(-20001,
                                  'validation of element(' || i.sls_cls_cd ||
                                  ') from sls_cls_cd_list ''' ||
                                  :new.sls_cls_cd_list ||
                                  ''' FAILED in SLS_CLS table');
        END IF;
      END LOOP;
    END IF;
    -- check Segments
    IF :new.sgmt_id_list IS NOT NULL THEN
      FOR i IN (SELECT TRIM(regexp_substr(col, '[^,]+', 1, LEVEL)) sgmt_id
                  FROM (SELECT :new.sgmt_id_list AS col FROM dual)
                CONNECT BY LEVEL <= length(regexp_replace(col, '[^,]+')) + 1) LOOP
        -- check if i.SGMT_ID is NUMBER
        IF TRIM(REPLACE(translate(i.sgmt_id, '0123456789', '0000000000'), '0', '')) IS NOT NULL THEN
          raise_application_error(-20001,
                                  'sgmt_id_list ''' || :new.sgmt_id_list ||
                                  ''' MUST contain only NUMERIC character(s)');
          EXIT;
        END IF;
        -- check if i.SGMT_ID is VALID in SGMT
        SELECT COUNT(1) INTO v_cnt FROM sgmt WHERE sgmt_id = i.sgmt_id;
        IF v_cnt <> 1 THEN
          raise_application_error(-20001,
                                  'validation of element(' || i.sgmt_id ||
                                  ') from sgmt_id_list ''' ||
                                  :new.sgmt_id_list ||
                                  ''' FAILED in SGMT table');
        END IF;
      END LOOP;
    END IF;
    --
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
      -- if not updating('CREAT_USER_ID') then      -- This does not work for inserting, so...
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
/
