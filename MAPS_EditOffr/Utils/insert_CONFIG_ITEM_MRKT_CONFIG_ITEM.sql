DELETE FROM mrkt_config_item WHERE config_item_id IN (9200, 9210, 9201);
DELETE FROM config_item WHERE config_item_id IN (9200, 9210, 9201);

INSERT INTO config_item (config_item_id, config_item_desc_txt, config_item_labl_txt, creat_user_id, creat_ts, last_updt_user_id, last_updt_ts, seq_nr)
VALUES (9200, 'Offr default values for offer manipulation', 'Offr default values for offer manipulation', USER, SYSDATE, USER, SYSDATE, 150);

INSERT INTO config_item (config_item_id, config_item_desc_txt, config_item_labl_txt, creat_user_id, creat_ts, last_updt_user_id, last_updt_ts, seq_nr)
VALUES (9210, 'Price point default values for offer manipulation', 'Price point default values for offer manipulation', USER, SYSDATE, USER, SYSDATE, 150);

INSERT INTO config_item (config_item_id, config_item_desc_txt, config_item_labl_txt, creat_user_id, creat_ts, last_updt_user_id, last_updt_ts, seq_nr)
VALUES (9201, 'Show sku details in new edit offer screen', 'Show/Hide sku details in new edit offer screen', USER, SYSDATE, USER, SYSDATE, 150);

COMMIT;
