DELETE FROM mrkt_config_item WHERE config_item_id IN (9200, 9210);
DELETE FROM config_item WHERE config_item_id IN (9200, 9210);

INSERT INTO config_item (config_item_id, config_item_desc_txt, config_item_labl_txt, seq_nr)
VALUES (9200, 'Offr default values for offer manipulation', 'Offr default values for offer manipulation', 150);

INSERT INTO config_item (config_item_id, config_item_desc_txt, config_item_labl_txt, seq_nr)
VALUES (9210, 'Price point default values for offer manipulation', 'Price point default values for offer manipulation', 150);

INSERT INTO mrkt_config_item (mrkt_id, config_item_id, mrkt_config_item_desc_txt, mrkt_config_item_labl_txt, mrkt_config_item_val_txt)
VALUES (73, 9200, 'Offr default values for offer manipulation', 'Offr default values for offer manipulation', '200,1,0,0,0,0,1,CMP,N,0,2,1');

INSERT INTO mrkt_config_item (mrkt_id, config_item_id, mrkt_config_item_desc_txt, mrkt_config_item_labl_txt, mrkt_config_item_val_txt)
VALUES (73, 9210, 'Price point default values for offer manipulation', 'Offr default values for offer manipulation', '147,10,1,0,0,0,1,0,1,1,N,0,0,0,0,1');

COMMIT;
