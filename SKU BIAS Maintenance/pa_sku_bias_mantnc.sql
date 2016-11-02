create or replace PACKAGE PA_SKU_BIAS AS 
  /*********************************************************
  * History
  * Created by   : Schiff Gy
  * Date         : 12/10/2016
  * Description  : First created 
  ******************************************************/

  PROCEDURE SET_SKU_BIAS(p_mrkt_id IN NUMBER,
                         p_sls_perd_id IN NUMBER,
                         p_sku_id IN NUMBER,
                         p_new_sku_bias IN NUMBER,
                         p_user_id IN VARCHAR2,
                         p_stus OUT NUMBER);
                                  
  FUNCTION GET_SKU_BIAS(p_mrkt_id IN NUMBER,
                        p_sls_perd_id IN NUMBER
                        ) RETURN OBJ_SKU_BIAS_MANTNC_TABLE PIPELINED;
                        
  FUNCTION GET_SKU_BIAS2(p_mrkt_id IN NUMBER,
                        p_sls_perd_id IN NUMBER,
                        p_sku_id_array IN NUMBER_ARRAY
                        )  RETURN OBJ_SKU_BIAS_MANTNC_TABLE PIPELINED;

END PA_SKU_BIAS;

  FUNCTION GET_SKU_BIAS(p_mrkt_id IN NUMBER,
                        p_sls_perd_id IN NUMBER
                        ) RETURN OBJ_SKU_BIAS_MANTNC_TABLE PIPELINED AS
  CURSOR cc IS
    WITH ACT_FSC AS
      (SELECT SKU_ID, MAX(STRT_PERD_ID) MAX_PERD_ID 
         FROM MRKT_FSC 
         WHERE MRKT_ID=p_mrkt_id AND STRT_PERD_ID<=p_sls_perd_id
         GROUP BY SKU_ID),
    MPSB AS           
      (SELECT PSB.SKU_ID,PSB.BIAS_PCT,PSB.LAST_UPDT_TS,
         U.USER_FRST_NM || ' ' || U.USER_LAST_NM LAST_UPDT_NM
         FROM MRKT_PERD_SKU_BIAS PSB
           LEFT JOIN MPS_USER U
             ON U.USER_NM=PSB.LAST_UPDT_USER_ID  
           WHERE PSB.MRKT_ID=p_mrkt_id AND PSB.SLS_PERD_ID=p_sls_perd_id
      ),
    SMS AS
      (SELECT S.SKU_ID,
        CASE when PA_MAPS_PUBLIC.get_sls_cls_cd(p_sls_perd_id, p_mrkt_id, s.avlbl_perd_id,
                         s.intrdctn_perd_id, s.demo_ofs_nr, s.demo_durtn_nr, s.new_durtn_nr,
                         s.stus_perd_id, s.dspostn_perd_id, s.on_stus_perd_id)!='-1' then 'A'
             else 'N' end as act_IND
      from MRKT_SKU S
      JOIN MRKT_PERD_SKU_PRC MPSP
        ON MPSP.MRKT_ID=p_mrkt_id AND MPSP.OFFR_PERD_ID=p_sls_perd_id
          AND MPSP.SKU_ID=S.SKU_ID AND MPSP.PRC_LVL_TYP_CD='RP'
      JOIN SKU_COST SC 
        ON SC.MRKT_ID=p_mrkt_id AND SC.OFFR_PERD_ID=p_sls_perd_id
          AND SC.SKU_ID=S.SKU_ID AND COST_TYP='P'
        WHERE S.MRKT_ID=p_mrkt_id AND S.DLTD_IND NOT IN('Y','y')
      ),
    CMP_OFFER AS
      (SELECT OFFR_ID FROM OFFR
         WHERE MRKT_ID=68 AND VER_ID = 0 AND OFFR_TYP='CMP'
           AND OFFR_PERD_ID =p_sls_perd_id),
    MF AS
      ( SELECT SKU_ID,FSC_CD,STRT_PERD_ID  FROM MRKT_FSC
          WHERE MRKT_ID=p_mrkt_id)
    SELECT OBJ_SKU_BIAS_MANTNC_LINE(
      FSC.FSC_CD,
      FSC.PROD_DESC_TXT,
      S.SKU_ID,
      S.LCL_SKU_NM,
      SMS.ACT_IND,
	    DECODE((SELECT count(*) FROM OFFR_SKU_LINE OSL 
              WHERE OSL.OFFR_ID in( SELECT OFFR_ID FROM CMP_OFFER)
               AND OSL.DLTD_IND NOT IN ('Y','y')
               AND OSL.SKU_ID=S.SKU_ID),0,'N','P'),
      PSB.BIAS_PCT,
      PSB.LAST_UPDT_TS,
      PSB.LAST_UPDT_NM
      ) cline
    FROM SMS
      JOIN MRKT_SKU S ON S.MRKT_ID=p_mrkt_id AND S.SKU_ID=SMS.SKU_ID
      LEFT JOIN MPSB PSB
        ON PSB.SKU_ID=S.SKU_ID
      LEFT JOIN ACT_FSC ACT
        ON ACT.SKU_ID=S.SKU_ID
      JOIN MF ON MF.SKU_ID=S.SKU_ID AND MF.STRT_PERD_ID=ACT.MAX_PERD_ID 
      JOIN MRKT_FSC FSC
        ON FSC.FSC_CD=MF.FSC_CD AND FSC.MRKT_ID=p_mrkt_id AND FSC.STRT_PERD_ID=MF.STRT_PERD_ID  
    ;
  BEGIN

    FOR rec in cc LOOP
      pipe row(rec.cline);
    END LOOP;
  END GET_SKU_BIAS;
  
  FUNCTION GET_SKU_BIAS2(p_mrkt_id IN NUMBER,
                        p_sls_perd_id IN NUMBER,
                        p_sku_id_array IN NUMBER_ARRAY
                        )  RETURN OBJ_SKU_BIAS_MANTNC_TABLE PIPELINED AS
  CURSOR cc IS
    WITH ACT_FSC AS
      (SELECT MRKT_ID, SKU_ID, MAX(STRT_PERD_ID) MAX_PERD_ID 
                 FROM MRKT_FSC 
                 WHERE MRKT_ID=p_mrkt_id AND STRT_PERD_ID<=p_sls_perd_id
                   AND SKU_ID in (select column_value from table( p_sku_id_array))
                 GROUP BY MRKT_ID,SKU_ID),
    MPSB AS           
      (SELECT * FROM MRKT_PERD_SKU_BIAS
                  WHERE MRKT_ID=p_mrkt_id AND SLS_PERD_ID=p_sls_perd_id
                    AND SKU_ID in (select column_value from table( p_sku_id_array))
      ),
    SMS AS
      (SELECT S.MRKT_ID,S.SKU_ID
      from MRKT_SKU S
      JOIN MRKT_PERD_SKU_PRC MPSP
        ON MPSP.MRKT_ID=p_mrkt_id AND MPSP.OFFR_PERD_ID=p_sls_perd_id
          AND MPSP.SKU_ID=S.SKU_ID AND MPSP.PRC_LVL_TYP_CD='RP'
      JOIN SKU_COST SC 
        ON SC.MRKT_ID=p_mrkt_id AND SC.OFFR_PERD_ID=p_sls_perd_id
          AND SC.SKU_ID=S.SKU_ID AND COST_TYP='P'
        WHERE S.MRKT_ID=p_mrkt_id AND S.DLTD_IND NOT IN('Y','y')
          AND S.SKU_ID in (select column_value from table( p_sku_id_array))
          AND (PA_MAPS_PUBLIC.get_sls_cls_cd(p_sls_perd_id, p_mrkt_id, s.avlbl_perd_id,
                         s.intrdctn_perd_id, s.demo_ofs_nr, s.demo_durtn_nr, s.new_durtn_nr,
                         s.stus_perd_id, s.dspostn_perd_id, s.on_stus_perd_id)!='-1')),
    CMP_OFFER AS
      (SELECT OFFR_ID FROM OFFR
         WHERE MRKT_ID=p_mrkt_id AND VER_ID = 0 AND OFFR_TYP='CMP'
           AND OFFR_PERD_ID =p_sls_perd_id)
    SELECT OBJ_SKU_BIAS_MANTNC_LINE(
      FSC.FSC_CD,
      FSC.PROD_DESC_TXT,
      S.SKU_ID,
      S.LCL_SKU_NM,
      'A',
	    DECODE((SELECT count(*) FROM OFFR_SKU_LINE OSL 
              WHERE OSL.OFFR_ID in( SELECT OFFR_ID FROM CMP_OFFER)
               AND OSL.DLTD_IND NOT IN ('Y','y')
               AND OSL.SKU_ID=S.SKU_ID),0,'N','P'),
      PSB.BIAS_PCT,
      PSB.CREAT_TS,
      PSB.CREAT_USER_ID,
      PSB.LAST_UPDT_TS,
      U.USER_FRST_NM || ' ' || U.USER_LAST_NM
      ) cline
    FROM SMS
      JOIN MRKT_SKU S ON S.MRKT_ID=SMS.MRKT_ID AND S.SKU_ID=SMS.SKU_ID
      LEFT JOIN MPSB PSB
        ON PSB.MRKT_ID=S.MRKT_ID AND PSB.SKU_ID=S.SKU_ID
      LEFT JOIN ACT_FSC ACT
        ON ACT.MRKT_ID=p_mrkt_id AND ACT.SKU_ID=S.SKU_ID
      JOIN MRKT_FSC FSC
        ON FSC.MRKT_ID=p_mrkt_id AND FSC.SKU_ID=S.SKU_ID AND FSC.STRT_PERD_ID=ACT.MAX_PERD_ID
      LEFT JOIN MPS_USER U
        ON U.USER_NM=PSB.LAST_UPDT_USER_ID  
    ;
  BEGIN

    FOR rec in cc LOOP
      pipe row(rec.cline);
    END LOOP;
  END GET_SKU_BIAS2;
  
  PROCEDURE SET_SKU_BIAS(p_mrkt_id IN NUMBER,
                         p_sls_perd_id IN NUMBER,
                         p_sku_id IN NUMBER,
                         p_new_sku_bias IN NUMBER,
                         p_user_id IN VARCHAR2,
                         p_stus OUT NUMBER) AS
  -- TODO - check if the sku is active, if not refuse handling the record                       
  /*********************************************************
  * INPUT p_new_sku_bias IS NULL handled as p_new_sku_bias=100
  *
  * Possible OUT Values
  * 0 - success
  * 1 - New BIAS_PCT value is out of range 0 to 1000
  * 2 - database error in DELETE, UPDATE or INSERT statements
  * 3 - Obligatory foreign keys (MRKT_SKU and MRKT_PERD) not found
  * 4 - Inactive SKU, change refused 
  ******************************************************/
  c_new_sku_bias NUMBER;
  counter1 NUMBER;
  counter2 NUMBER;
  counter3 NUMBER;
  BEGIN
    p_STUS:=0;
    -- p_new_sku_bias IS NULL replace with 100.
    c_new_sku_bias:=nvl(p_new_sku_bias,100);
    -- c_new_sku_bias must be between 0 and 1000
    IF c_new_sku_bias between 0 AND 1000 THEN
      SELECT count(*) INTO counter1 FROM MRKT_PERD_SKU_PRC MPSP
        WHERE MPSP.MRKT_ID=p_mrkt_id AND MPSP.OFFR_PERD_ID=p_sls_perd_id
          AND MPSP.SKU_ID=p_sku_id AND MPSP.PRC_LVL_TYP_CD='RP';
      SELECT count(*) INTO counter2 FROM SKU_COST SC 
        WHERE SC.MRKT_ID=p_mrkt_id AND SC.OFFR_PERD_ID=p_sls_perd_id
          AND SC.SKU_ID=p_sku_id AND COST_TYP='P';
      SELECT CASE WHEN PA_MAPS_PUBLIC.get_sls_cls_cd(p_sls_perd_id, p_mrkt_id, s.avlbl_perd_id,
                         s.intrdctn_perd_id, s.demo_ofs_nr, s.demo_durtn_nr, s.new_durtn_nr,
                         s.stus_perd_id, s.dspostn_perd_id, s.on_stus_perd_id)!='-1' THEN 1
                  ELSE 0 END
        INTO counter3 FROM MRKT_SKU S
          WHERE S.MRKT_ID=p_mrkt_id AND S.SKU_ID=p_sku_id;
      IF counter1+counter2+counter3=3 THEN -- active SKU
    -- c_new_sku_bias=100 means delete, the record if exists
        IF c_new_sku_bias=100 THEN
          BEGIN
          SAVEPOINT before_delete;
            DELETE FROM MRKT_PERD_SKU_BIAS
              WHERE MRKT_ID=p_mrkt_id AND SLS_PERD_ID=p_sls_perd_id AND SKU_ID=p_sku_id;
          EXCEPTION WHEN OTHERS THEN ROLLBACK TO before_delete; p_stus:=2; -- Database Error
	      END;
    -- otherwise upsert using p_new_bias
        ELSE
          SELECT count(*) INTO counter1 FROM MRKT_PERD WHERE MRKT_ID=p_mrkt_id AND PERD_ID=p_sls_perd_id;
          SELECT count(*) INTO counter2 FROM MRKT_SKU WHERE MRKT_ID=p_mrkt_id AND SKU_ID=p_sku_id;
          IF counter1+counter2=2 THEN
            BEGIN
            SAVEPOINT before_upsert; 		 
            MERGE INTO MRKT_PERD_SKU_BIAS trgt
              USING (SELECT p_mrkt_id t1,p_sls_perd_id t2,p_sku_id t3 FROM dual) src
                ON (trgt.MRKT_ID=src.t1 AND trgt.SLS_PERD_ID=src.t2 AND trgt.SKU_ID=src.t3)
              WHEN MATCHED THEN
                UPDATE SET trgt.BIAS_PCT = c_new_sku_bias, trgt.LAST_UPDT_USER_ID=p_user_id
              WHEN NOT MATCHED THEN
                INSERT (MRKT_ID,SLS_PERD_ID,SKU_ID,BIAS_PCT,LAST_UPDT_USER_ID)
                  VALUES (p_mrkt_id,p_sls_perd_id,p_sku_id,c_new_sku_bias,p_user_id);
	          EXCEPTION WHEN OTHERS THEN ROLLBACK TO before_changes; p_stus:=2; -- Database Error
          END;	
          ELSE p_STUS:=3; -- Missing foreign keys
          END IF;
        END IF;
      ELSE p_STUS:=4; -- Passive SKU
      END IF;
    ELSE p_STUS:=1; -- Negative value
    END IF;
  END SET_SKU_BIAS;
                                  
END PA_SKU_BIAS;