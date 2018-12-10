BEGIN
  FOR mrkts IN (SELECT DISTINCT mrkt_id FROM mrkt_perd) LOOP
    UPDATE mrkt_config_item i
       SET i.mrkt_config_item_val_txt = '10,1,0,0,0,0,1,CMP,N,0,2,1',
           i.last_updt_user_id = USER,
           i.last_updt_ts = SYSDATE
     WHERE i.mrkt_id = mrkts.mrkt_id
       AND i.config_item_id = 9200;
  END LOOP;

  COMMIT;
END;
/
