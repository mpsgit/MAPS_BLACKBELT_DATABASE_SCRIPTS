DELETE FROM mrkt_config_item WHERE config_item_id = 13100;
DELETE FROM config_item WHERE config_item_id = 13100;

INSERT INTO config_item (config_item_id, config_item_desc_txt, config_item_labl_txt, creat_user_id, creat_ts, last_updt_user_id, last_updt_ts, seq_nr)
VALUES (13100, 'Pagination planning - disable ML', 'Pagination planning - disable ML', USER, SYSDATE, USER, SYSDATE, 1);

BEGIN
  FOR mrkt_rec IN (SELECT DISTINCT mrkt_id FROM mrkt_perd) LOOP
    INSERT INTO mrkt_config_item (
      mrkt_id, config_item_id, mrkt_config_item_desc_txt, mrkt_config_item_labl_txt,
      mrkt_config_item_val_txt, creat_user_id, creat_ts, last_updt_user_id, last_updt_ts)
    VALUES (
      mrkt_rec.mrkt_id, 13100, 'Pagination planning - disable ML', 'Pagination planning - disable ML',
      'Y', USER, SYSDATE, USER, SYSDATE);
  END LOOP;
END;
/

COMMIT;
