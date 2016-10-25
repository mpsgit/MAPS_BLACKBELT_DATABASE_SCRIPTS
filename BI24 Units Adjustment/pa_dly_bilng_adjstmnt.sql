create or replace PACKAGE PA_DLY_BILNG_ADJSTMNT AS 
  /*********************************************************
  * History
  * Created by   : Schiff Gy
  * Date         : 12/10/2016
  * Description  : First created 
  ******************************************************/

  PROCEDURE SET_DLY_BILNG_ADJSTMNT(p_mrkt_id IN NUMBER,
                         p_sls_perd_id IN NUMBER,
                         p_offr_perd_id IN NUMBER,
                         p_prcsng_dt IN DATE,
                         p_new_bi24_units IN NUMBER,
                         p_user_id IN VARCHAR2,
                         p_stus OUT NUMBER);
                                  
  FUNCTION GET_DLY_BILNG_ADJSTMNT(p_mrkt_id IN NUMBER,
                        p_sls_perd_id IN NUMBER,
                        p_offr_perd_id IN NUMBER,
                        p_prcsng_dt IN DATE
                        ) RETURN OBJ_DLY_BILNG_ADJSTMNT_TABLE PIPELINED;                                

END PA_DLY_BILNG_ADJSTMNT;

create or replace PACKAGE BODY PA_DLY_BILNG_ADJSTMNT AS

  FUNCTION GET_DLY_BILNG_ADJSTMNT(p_mrkt_id IN NUMBER,
                        p_sls_perd_id IN NUMBER,
                        p_offr_perd_id IN NUMBER,
                        p_prcsng_dt IN DATE
                        ) RETURN OBJ_DLY_BILNG_ADJSTMNT_TABLE PIPELINED AS
  CURSOR cc IS
    SELECT OBJ_DLY_BILNG_ADJSTMNT_LINE(
      DB.DLY_BILNG_ID,
      DB.FSC_CD,
      PA_MAPS_PUBLIC.get_fsc_desc(p_mrkt_id,p_offr_perd_id,DB.FSC_CD),
      DB.SKU_ID,
      S.LCL_SKU_NM,
      DB.SLS_PRC_AMT,
      DB.NR_FOR_QTY,
      DB.SLS_PRC_AMT/DB.NR_FOR_QTY,
--      'A',
      DECODE((SELECT 1 FROM dual WHERE
                EXISTS( SELECT SKU_PRC_AMT FROM MRKT_PERD_SKU_PRC MPSP
                   WHERE MPSP.MRKT_ID=p_mrkt_id AND MPSP.OFFR_PERD_ID=p_sls_perd_id
                    AND MPSP.SKU_ID=S.SKU_ID AND MPSP.PRC_LVL_TYP_CD='RP' )
                AND EXISTS( SELECT HOLD_COSTS_IND FROM SKU_COST SC 
                   WHERE SC.MRKT_ID=p_mrkt_id AND SC.OFFR_PERD_ID=p_offr_perd_id
                    AND SC.SKU_ID=S.SKU_ID AND COST_TYP='P')
                AND (PA_MAPS_PUBLIC.get_sls_cls_cd(p_offr_perd_id, p_mrkt_id, s.avlbl_perd_id,
                         s.intrdctn_perd_id, s.demo_ofs_nr, s.demo_durtn_nr, s.new_durtn_nr,
                         s.stus_perd_id, s.dspostn_perd_id, s.on_stus_perd_id)!='-1')),'1','A','N'),
	    DECODE((SELECT count(*) FROM OFFR_SKU_LINE OSL 
              WHERE OSL.OFFR_ID in( SELECT OFFR_ID FROM OFFR O
                                    WHERE O.MRKT_ID=p_mrkt_id
                                     AND O.VER_ID = 0
                                     AND O.OFFR_TYP='CMP'
                                     AND O.OFFR_PERD_ID =p_offr_perd_id)
               AND OSL.DLTD_IND NOT IN ('Y','y')
               AND OSL.SKU_ID=S.SKU_ID),0,'N','P'),
      DB.UNIT_QTY,
      DBAT.UNIT_QTY,
      DBAT.LAST_UPDT_USER_ID,
      DBAT.LAST_UPDT_TS) cline
    FROM DLY_BILNG DB
      LEFT JOIN MRKT_SKU S
        ON S.MRKT_ID=p_mrkt_id AND S.SKU_ID=DB.SKU_ID
      LEFT JOIN DLY_BILNG_ADJSTMNT DBAT
        ON DBAT.DLY_BILNG_ID=DB.DLY_BILNG_ID
    WHERE DB.MRKT_ID=p_mrkt_id AND DB.SLS_PERD_ID=p_sls_perd_id
      AND DB.OFFR_PERD_ID=p_offr_perd_id AND trunc(DB.PRCSNG_DT)=trunc(p_prcsng_dt)
--    ORDER BY DB.DLY_BILNG_ID,db.SKU_ID
    ;
  BEGIN

    FOR rec in cc LOOP
      pipe row(rec.cline);
    END LOOP; 
  END GET_DLY_BILNG_ADJSTMNT;
  
  PROCEDURE SET_DLY_BILNG_ADJSTMNT(p_mrkt_id IN NUMBER,
                         p_sls_perd_id IN NUMBER,
                         p_offr_perd_id IN NUMBER,
                         p_prcsng_dt IN DATE,
                         p_new_bi24_units IN NUMBER,
                         p_user_id IN VARCHAR2,
                         p_stus OUT NUMBER) AS
  /*********************************************************
  * INPUT p_new_sku_bias IS NULL handled as p_new_sku_bias=100
  *
  * Possible OUT Values
  * 0 - success
  * 1 - New BIAS_PCT value is out of range 0 to 1000
  * 2 - database error in DELETE, UPDATE or INSERT statements
  * 3 - Obligatory foreign keys (MRKT_SKU and MRKT_PERD) not found
  ******************************************************/
/*  c_new_sku_bias NUMBER;
  counter1 NUMBER;
  counter2 NUMBER;
  BEGIN
    -- p_new_sku_bias IS NULL replace with 100.
    c_new_sku_bias:=nvl(p_new_sku_bias,100);
    -- c_new_sku_bias must be between 0 and 1000
    IF c_new_sku_bias between 0 AND 1000 THEN
    -- c_new_sku_bias=100 means delete, the record if exists
      IF c_new_sku_bias=100 THEN
	     BEGIN
        SAVEPOINT before_delete;
        DELETE FROM MRKT_PERD_SKU_BIAS
          WHERE MRKT_ID=p_mrkt_id AND SLS_PERD_ID=p_sls_perd_id AND SKU_ID=p_sku_id;
       EXCEPTION WHEN OTHERS THEN ROLLBACK TO before_delete; p_stus:=2;
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
	       EXCEPTION WHEN OTHERS THEN ROLLBACK TO before_changes; p_stus:=2;
		     END;	
        ELSE p_STUS:=3;
        END IF;
      END IF;
    ELSE p_STUS:=1;  
    END IF;
    EXCEPTION WHEN OTHERS THEN ROLLBACK TO before_changes; p_stus:=2;*/BEGIN NULL;
  END SET_DLY_BILNG_ADJSTMNT;
                                  
END PA_DLY_BILNG_ADJSTMNT;