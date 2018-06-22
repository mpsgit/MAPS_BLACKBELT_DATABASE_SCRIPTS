BEGIN

  FOR I IN (SELECT DISTINCT mrkt_id FROM mrkt_perd) LOOP

    INSERT INTO mrkt_config_item (mrkt_id, config_item_id, mrkt_config_item_desc_txt, mrkt_config_item_labl_txt, mrkt_config_item_val_txt)
    VALUES (i.mrkt_id, 9200, 'Offr default values for offer manipulation', 'Offr default values for offer manipulation', '200,1,0,0,0,0,1,CMP,N,0,2,1');
  
    INSERT INTO mrkt_config_item (mrkt_id, config_item_id, mrkt_config_item_desc_txt, mrkt_config_item_labl_txt, mrkt_config_item_val_txt)
    VALUES (i.mrkt_id, 9210, 'Price point default values for offer manipulation', 'Offr default values for offer manipulation', '147,10,1,0,0,0,1,0,1,1,N,0,0,0,0,1');

    dbms_output.put_line(i.mrkt_id);
  END LOOP;
END;
/

COMMIT;
