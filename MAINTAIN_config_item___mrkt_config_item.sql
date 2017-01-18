-- UPDATE
-- config_item
insert into config_item (CONFIG_ITEM_ID, CONFIG_ITEM_DESC_TXT, CONFIG_ITEM_LABL_TXT, SEQ_NR)
values (12005, 'TREND ALLOCATION ENABLED (73)', 'TREND ALLOCATION ENABLED (73)', 105);
-- mrkt_config_item
UPDATE mrkt_config_item
   SET MRKT_CONFIG_ITEM_VAL_TXT = 'N'
 WHERE config_item_id = 12003
   AND mrkt_id = 73;
-- INSERT
insert into mrkt_config_item (MRKT_ID, CONFIG_ITEM_ID, MRKT_CONFIG_ITEM_DESC_TXT, MRKT_CONFIG_ITEM_LABL_TXT, MRKT_CONFIG_ITEM_VAL_TXT)
values (73, 12005, 'TREND ALLOCATION ENABLED (73)', 'TREND ALLOCATION ENABLED (73)', 'Y');
-- AUDIT:
-- config_item_audit
-- INSERT:
-- config_item_audit
insert into config_item_audit (CONFIG_ITEM_ID, CONFIG_ITEM_DESC_TXT, CONFIG_ITEM_LABL_TXT, SEQ_NR)
values (12005, 'TREND ALLOCATION ENABLED (73)', 'TREND ALLOCATION ENABLED (73)', 100);
-- mrkt_config_item_audit
-- INSERT
insert into mrkt_config_item_audit (MRKT_ID, CONFIG_ITEM_ID, MRKT_CONFIG_ITEM_DESC_TXT, MRKT_CONFIG_ITEM_LABL_TXT, MRKT_CONFIG_ITEM_VAL_TXT)
values (73, 12003, 'TREND ALLOCATION ENABLED', 'TREND ALLOCATION ENABLED', 'N');
-- mrkt_config_item_audit
insert into mrkt_config_item_audit (MRKT_ID, CONFIG_ITEM_ID, MRKT_CONFIG_ITEM_DESC_TXT, MRKT_CONFIG_ITEM_LABL_TXT, MRKT_CONFIG_ITEM_VAL_TXT)
values (73, 12005, 'TREND ALLOCATION ENABLED (73)', 'TREND ALLOCATION ENABLED (73)', 'Y');
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
            FROM mrkt_config_item mci
           WHERE mci.mrkt_id = mm.mrkt_id
             AND mci.config_item_id = 10000);