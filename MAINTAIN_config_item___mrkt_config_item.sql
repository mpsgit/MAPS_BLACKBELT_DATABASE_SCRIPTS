-- BEFORE
-- config_item
SELECT * FROM config_item where config_item_id > 10000 ORDER BY config_item_id;
-- mrkt_config_item
SELECT *
  FROM mrkt_config_item
 WHERE config_item_id > 10000
 ORDER BY config_item_id, mrkt_id, mrkt_config_item_val_txt;
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
-- AFTER
-- mrkt_config_item
SELECT *
  FROM mrkt_config_item
 WHERE config_item_id > 10000
 ORDER BY config_item_id, mrkt_id, mrkt_config_item_val_txt;
-- config_item
SELECT *
  FROM config_item
 WHERE config_item_id > 10000
 ORDER BY config_item_id;
----------------------------
-- AUDIT:
-- config_item_audit
select * from config_item_audit where config_item_id > 10000 ORDER BY config_item_id;
-- INSERT:
-- config_item_audit
insert into config_item_audit (CONFIG_ITEM_ID, CONFIG_ITEM_DESC_TXT, CONFIG_ITEM_LABL_TXT, SEQ_NR)
values (12005, 'TREND ALLOCATION ENABLED (73)', 'TREND ALLOCATION ENABLED (73)', 100);
-- mrkt_config_item_audit
select * from mrkt_config_item_audit where config_item_id > 10000 ORDER BY config_item_id;
-- INSERT:
insert into mrkt_config_item_audit (MRKT_ID, CONFIG_ITEM_ID, MRKT_CONFIG_ITEM_DESC_TXT, MRKT_CONFIG_ITEM_LABL_TXT, MRKT_CONFIG_ITEM_VAL_TXT)
values (73, 12003, 'TREND ALLOCATION ENABLED', 'TREND ALLOCATION ENABLED', 'N');
-- mrkt_config_item_audit
insert into mrkt_config_item_audit (MRKT_ID, CONFIG_ITEM_ID, MRKT_CONFIG_ITEM_DESC_TXT, MRKT_CONFIG_ITEM_LABL_TXT, MRKT_CONFIG_ITEM_VAL_TXT)
values (73, 12005, 'TREND ALLOCATION ENABLED (73)', 'TREND ALLOCATION ENABLED (73)', 'Y');
