DELETE FROM mrkt_config_item WHERE config_item_id IN (9200, 9210, 9201);
DELETE FROM config_item WHERE config_item_id IN (9200, 9210, 9201);

COMMIT;
