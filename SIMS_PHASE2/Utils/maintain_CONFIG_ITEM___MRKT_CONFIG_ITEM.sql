-- UPDATE
-- CLEAN UP
delete from mrkt_config_item where CONFIG_ITEM_ID in (12005, 12006);
delete from config_item where CONFIG_ITEM_ID in (12005, 12006);
delete from mrkt_config_item_audit where CONFIG_ITEM_ID in (12005, 12006);
delete from mrkt_config_item_audit where CONFIG_ITEM_ID = 12003 and MRKT_ID = 73;
delete from config_item_audit where CONFIG_ITEM_ID in (12005, 12006);
-- CONFIG
-- config_item
insert into config_item (CONFIG_ITEM_ID, CONFIG_ITEM_DESC_TXT, CONFIG_ITEM_LABL_TXT, SEQ_NR)
values (12005, 'PA_TREND_ALLOCATION ENABLED', 'PA_TREND_ALLOCATION ENABLED', 105);
insert into config_item (CONFIG_ITEM_ID, CONFIG_ITEM_DESC_TXT, CONFIG_ITEM_LABL_TXT, SEQ_NR)
values (12006, 'PA_TREND_ALLOCATION period list', 'PA_TREND_ALLOCATION period list', 105);
-- mrkt_config_item
UPDATE mrkt_config_item
   SET MRKT_CONFIG_ITEM_VAL_TXT = 'N'
 WHERE config_item_id = 12003
   AND mrkt_id = 73;
-- INSERT
-- 12005
insert into mrkt_config_item (MRKT_ID, CONFIG_ITEM_ID, MRKT_CONFIG_ITEM_DESC_TXT, MRKT_CONFIG_ITEM_LABL_TXT, MRKT_CONFIG_ITEM_VAL_TXT)
values (73, 12005, 'PA_TREND_ALLOCATION ENABLED', 'PA_TREND_ALLOCATION ENABLED', 'Y');
insert into mrkt_config_item (MRKT_ID, CONFIG_ITEM_ID, MRKT_CONFIG_ITEM_DESC_TXT, MRKT_CONFIG_ITEM_LABL_TXT, MRKT_CONFIG_ITEM_VAL_TXT)
values (68, 12005, 'PA_TREND_ALLOCATION ENABLED', 'PA_TREND_ALLOCATION ENABLED', 'Y');
insert into mrkt_config_item (MRKT_ID, CONFIG_ITEM_ID, MRKT_CONFIG_ITEM_DESC_TXT, MRKT_CONFIG_ITEM_LABL_TXT, MRKT_CONFIG_ITEM_VAL_TXT)
values (50, 12005, 'PA_TREND_ALLOCATION ENABLED', 'PA_TREND_ALLOCATION ENABLED', 'Y');
-- 12006
insert into mrkt_config_item (MRKT_ID, CONFIG_ITEM_ID, MRKT_CONFIG_ITEM_DESC_TXT, MRKT_CONFIG_ITEM_LABL_TXT, MRKT_CONFIG_ITEM_VAL_TXT)
values (73, 12006, 'PA_TREND_ALLOCATION period list', 'PA_TREND_ALLOCATION period list', '-1,0,1');
insert into mrkt_config_item (MRKT_ID, CONFIG_ITEM_ID, MRKT_CONFIG_ITEM_DESC_TXT, MRKT_CONFIG_ITEM_LABL_TXT, MRKT_CONFIG_ITEM_VAL_TXT)
values (68, 12006, 'PA_TREND_ALLOCATION period list', 'PA_TREND_ALLOCATION period list', '-1,0,1');
insert into mrkt_config_item (MRKT_ID, CONFIG_ITEM_ID, MRKT_CONFIG_ITEM_DESC_TXT, MRKT_CONFIG_ITEM_LABL_TXT, MRKT_CONFIG_ITEM_VAL_TXT)
values (50, 12006, 'PA_TREND_ALLOCATION period list', 'PA_TREND_ALLOCATION period list', '-20,-5,0,3,4');
-- AUDIT:
-- config_item_audit
-- INSERT:
-- config_item_audit
insert into config_item_audit (CONFIG_ITEM_ID, CONFIG_ITEM_DESC_TXT, CONFIG_ITEM_LABL_TXT, SEQ_NR)
values (12005, 'PA_TREND_ALLOCATION ENABLED', 'PA_TREND_ALLOCATION ENABLED', 100);
-- mrkt_config_item_audit
-- INSERT
-- 12005
insert into mrkt_config_item_audit (MRKT_ID, CONFIG_ITEM_ID, MRKT_CONFIG_ITEM_DESC_TXT, MRKT_CONFIG_ITEM_LABL_TXT, MRKT_CONFIG_ITEM_VAL_TXT)
values (73, 12003, 'TREND ALLOCATION ENABLED', 'TREND ALLOCATION ENABLED', 'N');
-- mrkt_config_item_audit
-- 12005
insert into mrkt_config_item_audit (MRKT_ID, CONFIG_ITEM_ID, MRKT_CONFIG_ITEM_DESC_TXT, MRKT_CONFIG_ITEM_LABL_TXT, MRKT_CONFIG_ITEM_VAL_TXT)
values (73, 12005, 'PA_TREND_ALLOCATION ENABLED', 'PA_TREND_ALLOCATION ENABLED', 'Y');
insert into mrkt_config_item_audit (MRKT_ID, CONFIG_ITEM_ID, MRKT_CONFIG_ITEM_DESC_TXT, MRKT_CONFIG_ITEM_LABL_TXT, MRKT_CONFIG_ITEM_VAL_TXT)
values (68, 12005, 'PA_TREND_ALLOCATION ENABLED', 'PA_TREND_ALLOCATION ENABLED', 'Y');
insert into mrkt_config_item_audit (MRKT_ID, CONFIG_ITEM_ID, MRKT_CONFIG_ITEM_DESC_TXT, MRKT_CONFIG_ITEM_LABL_TXT, MRKT_CONFIG_ITEM_VAL_TXT)
values (50, 12005, 'PA_TREND_ALLOCATION ENABLED', 'PA_TREND_ALLOCATION ENABLED', 'Y');
-- 12006
insert into mrkt_config_item_audit (MRKT_ID, CONFIG_ITEM_ID, MRKT_CONFIG_ITEM_DESC_TXT, MRKT_CONFIG_ITEM_LABL_TXT, MRKT_CONFIG_ITEM_VAL_TXT)
values (73, 12006, 'PA_TREND_ALLOCATION period list', 'PA_TREND_ALLOCATION period list', '-1,0,1');
insert into mrkt_config_item_audit (MRKT_ID, CONFIG_ITEM_ID, MRKT_CONFIG_ITEM_DESC_TXT, MRKT_CONFIG_ITEM_LABL_TXT, MRKT_CONFIG_ITEM_VAL_TXT)
values (68, 12006, 'PA_TREND_ALLOCATION period list', 'PA_TREND_ALLOCATION period list', '-1,0,1');
insert into mrkt_config_item_audit (MRKT_ID, CONFIG_ITEM_ID, MRKT_CONFIG_ITEM_DESC_TXT, MRKT_CONFIG_ITEM_LABL_TXT, MRKT_CONFIG_ITEM_VAL_TXT)
values (50, 12006, 'PA_TREND_ALLOCATION period list', 'PA_TREND_ALLOCATION period list', '-20,-5,0,3,4');
--
--
--
insert into CONFIG_ITEM (CONFIG_ITEM_ID, CONFIG_ITEM_DESC_TXT, CONFIG_ITEM_LABL_TXT, SEQ_NR)
values (10000, 'Sales Type Id for Trend Allocation', 'Sales Type Id for Trend Allocation', 999);
--
INSERT INTO mrkt_config_item
  (mrkt_id,
   config_item_id,
   mrkt_config_item_desc_txt,
   mrkt_config_item_labl_txt,
   mrkt_config_item_val_txt)
  SELECT mm.mrkt_id,
         10000,
         'Sales Type Id for Trend Allocation',
         'Sales Type Id for Trend Allocation',
         6
    FROM mrkt mm
   WHERE NOT EXISTS (SELECT mrkt_id
            FROM mrkt_config_item mci
           WHERE mci.mrkt_id = mm.mrkt_id
             AND mci.config_item_id = 10000);
-- AUDIT:
-- config_item_audit
-- INSERT:
-- config_item_audit
insert into config_item_audit (CONFIG_ITEM_ID, CONFIG_ITEM_DESC_TXT, CONFIG_ITEM_LABL_TXT, SEQ_NR)
values (10000, 'Sales Type Id for Trend Allocation', 'Sales Type Id for Trend Allocation', 999);
-- mrkt_config_item_audit
-- INSERT
INSERT INTO mrkt_config_item_audit
  (mrkt_id,
   config_item_id,
   mrkt_config_item_desc_txt,
   mrkt_config_item_labl_txt,
   mrkt_config_item_val_txt)
  SELECT mm.mrkt_id,
         10000,
         'Sales Type Id for Trend Allocation',
         'Sales Type Id for Trend Allocation',
         6
    FROM mrkt mm
   WHERE NOT EXISTS (SELECT mrkt_id
            FROM mrkt_config_item_audit mcia
           WHERE mcia.mrkt_id = mm.mrkt_id
             AND mcia.config_item_id = 10000);
       
INSERT INTO CONFIG_ITEM (CONFIG_ITEM_ID, CONFIG_ITEM_DESC_TXT, CONFIG_ITEM_LABL_TXT, SEQ_NR)
values (10001, 'Estimate Type to Show At Trend Allocation', 'Estimate Type to Show At Trend Allocation', 999);
--
INSERT INTO mrkt_config_item
  (mrkt_id,
   config_item_id,
   mrkt_config_item_desc_txt,
   mrkt_config_item_labl_txt,
   mrkt_config_item_val_txt)
  SELECT mm.mrkt_id,
         10001,
         'Estimate Type to Show At Trend Allocation',
         'Estimate Type to Show At Trend Allocation',
         case when mm.mrkt_id=73 then  2 else 1 end
    FROM mrkt mm
   WHERE NOT EXISTS (SELECT mrkt_id
            FROM mrkt_config_item mci
           WHERE mci.mrkt_id = mm.mrkt_id
             AND mci.config_item_id = 10001);
-- AUDIT:
-- config_item_audit
-- INSERT:
-- config_item_audit
insert into config_item_audit (CONFIG_ITEM_ID, CONFIG_ITEM_DESC_TXT, CONFIG_ITEM_LABL_TXT, SEQ_NR)
values (10001, 'Estimate Type to Show At Trend Allocation', 'Estimate Type to Show At Trend Allocation', 999);
-- mrkt_config_item_audit
-- INSERT
INSERT INTO mrkt_config_item_audit
  (mrkt_id,
   config_item_id,
   mrkt_config_item_desc_txt,
   mrkt_config_item_labl_txt,
   mrkt_config_item_val_txt)
  SELECT mm.mrkt_id,
         10001,
         'Estimate Type to Show At Trend Allocation',
         'Estimate Type to Show At Trend Allocation',
         case when mm.mrkt_id=73 then  2 else 1 end
    FROM mrkt mm
   WHERE NOT EXISTS (SELECT mrkt_id
            FROM mrkt_config_item_audit mcia
           WHERE mcia.mrkt_id = mm.mrkt_id
             AND mcia.config_item_id = 10001);
