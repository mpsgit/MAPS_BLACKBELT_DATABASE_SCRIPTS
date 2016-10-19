create or replace 
PACKAGE CASH_VLU_MANTNC IS
  /*********************************************************
  * History
  * Created by   : Schiff Gy
  * Date         : 12/10/2016
  * Description  : First created 
  ******************************************************/

  PROCEDURE SET_CASH_VAL_R_FACTOR(p_mrkt_id IN NUMBER,
                                  p_sls_perd_id IN NUMBER,
                                  p_cash_val IN NUMBER,
                                  p_r_factor IN NUMBER,
                                  p_user_id IN VARCHAR2,
                                  p_stus OUT NUMBER);
                                  
  FUNCTION GET_CASH_VAL(p_mrkt_id IN NUMBER,
                        p_sls_perd_list IN NUMBER_ARRAY
                        ) RETURN OBJ_CASH_VAL_MANTNC_TABLE PIPELINED;                                
  
END CASH_VLU_MANTNC;


create or replace 
PACKAGE BODY CASH_VLU_MANTNC AS
  /*********************************************************
  * History
  * Created by   : Schiff Gy
  * Date         : 12/10/2016
  * Description  : First created 
  ******************************************************/
FUNCTION get_local_prcsng_dt(p_mrkt_id NUMBER, p_sls_perd NUMBER) RETURN DATE;

FUNCTION GET_CASH_VAL(p_mrkt_id IN NUMBER,
                      p_sls_perd_list IN NUMBER_ARRAY
                      ) RETURN OBJ_CASH_VAL_MANTNC_TABLE PIPELINED IS                      
    
CURSOR cc IS
  SELECT OBJ_CASH_VAL_MANTNC_LINE(
    T.PERD_ID, SP.SCT_CASH_VAL, SP.SCT_R_FACTOR,
    CASE
      WHEN M.MAX_PERD_ID>t.PERD_ID THEN 'FINISHED_TRENDING'
      WHEN M.MAX_PERD_ID=t.PERD_ID THEN 'CURRENT_TRENDING'
      ELSE 'PLANNING'
    END,
    SP.LAST_UPDT_TS, U.USER_FRST_NM || ' ' || U.USER_LAST_NM) cline
  FROM (SELECT  MAX(SLS_PERD_ID) as MAX_PERD_ID FROM DLY_BILNG
                     WHERE MRKT_ID=p_mrkt_id GROUP BY MRKT_ID) M, MRKT_PERD T
    LEFT JOIN MRKT_SLS_PERD SP
      ON SP.MRKT_ID=T.MRKT_ID AND SP.SLS_PERD_ID=T.PERD_ID
    LEFT JOIN MPS_USER U
      ON U.USER_NM=SP.LAST_UPDT_USER_ID
  WHERE T.PERD_TYP='SC'
    AND T.MRKT_ID=p_mrkt_id
    AND T.PERD_ID IN((select column_value from table( p_sls_perd_list)))
  ORDER BY T.MRKT_ID, T.PERD_ID desc;
BEGIN

  FOR rec in cc LOOP
    pipe row(rec.cline);
  END LOOP;
END;  

PROCEDURE SET_CASH_VAL_R_FACTOR(p_mrkt_id IN NUMBER,
                                  p_sls_perd_id IN NUMBER,
                                  p_cash_val IN NUMBER,
                                  p_r_factor IN NUMBER,
                                  p_user_id IN VARCHAR2,
                                  p_stus OUT NUMBER) IS
  /*********************************************************
  * Possible OUT Values
  * 0 - success
  * 1 - New CASH_VAL value is negative
  * 2 - database error in UPDATE MRKT_SLS_PERD or INSERT INTO CASH_VAL_RF_HIST statements
  * 3 - record to update in table MRKT_SLS_PERD is readonly (has records in DLY_BILNG)
  * 4 - period to update is not defined yet in table MRKT_PERD
  ******************************************************/
  max_perd_id MRKT_SLS_PERD.SLS_PERD_ID%TYPE;
  local_prcsng_dt DATE:=NULL;                                
  old_sct_cash_val MRKT_SLS_PERD.SCT_CASH_VAL%TYPE;
  old_sct_r_factor MRKT_SLS_PERD.SCT_R_FACTOR%TYPE;
  mrkt_perd_exists NUMBER;
  wtd CHAR(1):='N';
BEGIN
  p_stus:=0;
  IF p_cash_val>0 THEN p_STUS:=1;
  ELSE
    SELECT MAX(SLS_PERD_ID) INTO max_perd_id
      FROM DLY_BILNG WHERE MRKT_ID=p_mrkt_id
      GROUP BY MRKT_ID;
    IF p_sls_perd_id > max_perd_id THEN
-- Read current record for supplied market/campaign from MRKT_SLS_PERD
      SELECT count(*) INTO mrkt_perd_exists 
        FROM MRKT_SLS_PERD WHERE MRKT_ID=p_mrkt_id AND SLS_PERD_ID=p_sls_perd_id;
      IF mrkt_perd_exists=1 THEN
        SELECT SCT_CASH_VAL,SCT_R_FACTOR 
        INTO  old_sct_cash_val,old_sct_r_factor
        FROM MRKT_SLS_PERD WHERE MRKT_ID=p_mrkt_id AND SLS_PERD_ID=p_sls_perd_id;
-- If either the Cash Value or the R-Factor is different to current values
        IF old_sct_cash_val=p_cash_val AND old_sct_r_factor=p_r_factor
          THEN wtd:='N'; -- No changes, nothing to do
          ELSE wtd:='U'; -- previous value exists and different, so update
        END IF;
      ELSE
        SELECT count(*) INTO mrkt_perd_exists 
          FROM MRKT_PERD WHERE MRKT_ID=p_mrkt_id AND PERD_ID=p_sls_perd_id;
        IF mrkt_perd_exists=1
          THEN wtd:='I'; -- The period still exists, Insert
          ELSE wtd:='E'; -- changing non-existing period
        END IF;
      END IF;  
      IF wtd = 'N' THEN NULL;
      ELSIF wtd = 'E' THEN p_stus:=4;
      ELSE
-- Create the missing local processing date value
        local_prcsng_dt:=CASH_VLU_MANTNC.get_local_prcsng_dt(p_mrkt_id, p_sls_perd_id);
        BEGIN
          SAVEPOINT before_changes;
-- Upsert the appropriate record in MRKT_SLS_PERD to set new values including local processing date
          IF wtd = 'U' THEN
            UPDATE MRKT_SLS_PERD
            SET MRKT_SLS_PERD.SCT_CASH_VAL=p_cash_val,
                MRKT_SLS_PERD.SCT_R_FACTOR=p_r_factor,
                MRKT_SLS_PERD.SCT_PRCSNG_DT=local_prcsng_dt,
                MRKT_SLS_PERD.LAST_UPDT_USER_ID=p_user_id
              WHERE MRKT_ID=p_mrkt_id AND SLS_PERD_ID=p_sls_perd_id;
          ELSE
            INSERT INTO MRKT_SLS_PERD(MRKT_ID,SLS_PERD_ID,SCT_CASH_VAL,SCT_PRCSNG_DT,LAST_UPDT_USER_ID)
              VALUES (p_mrkt_id,p_sls_perd_id,p_cash_val,local_prcsng_dt,p_user_id);
          END IF;
-- Insert a record into CASH_VAL_RF_HIST to record the previous values
          INSERT INTO CASH_VAL_RF_HIST(MRKT_ID,SLS_PERD_ID,CASH_VAL,PRCSNG_DT,LAST_UPDT_USER_ID)
            SELECT MRKT_ID,SLS_PERD_ID,p_cash_val,local_prcsng_dt,p_user_id 
              FROM MRKT_SLS_PERD WHERE MRKT_ID=p_mrkt_id AND SLS_PERD_ID=p_sls_perd_id;
        EXCEPTION WHEN OTHERS THEN ROLLBACK TO before_changes; p_stus:=2;
        END;
      END IF; 
    ELSE
      p_stus:=3;
    END IF;  
  END IF;
END;

FUNCTION get_local_prcsng_dt(p_mrkt_id NUMBER, p_sls_perd NUMBER) RETURN DATE IS
  processing_dt date:=NULL;
  actual_sls_perd number;
  before2_sls_perd number;
  bst_bilng_dt date;
  est_bilng_dt date;
  latest_prcsng_dt date;
  BEGIN
    WITH mrkt_perd2 AS
     (SELECT MRKT_ID, PERD_ID, row_number() OVER (ORDER BY PERD_ID) AS rn,
             BST_BILNG_DT
       FROM MRKT_PERD WHERE MRKT_ID=p_mrkt_id AND PERD_TYP='SC'),
    mrkt_perd3 AS
     (SELECT MRKT_ID, PERD_ID, row_number() OVER (ORDER BY PERD_ID) AS rn,
             EST_BILNG_DT
       FROM MRKT_PERD WHERE MRKT_ID=p_mrkt_id AND PERD_TYP='SC')  
     SELECT a1.PERD_ID, a2.PERD_ID, a1.BST_BILNG_DT, a2.EST_BILNG_DT
      INTO actual_sls_perd, before2_sls_perd, bst_bilng_dt, est_bilng_dt
      FROM mrkt_perd2 a1 LEFT JOIN mrkt_perd3 a2 ON a2.rn=a1.rn-2
      WHERE a1.PERD_ID=p_sls_perd;
    SELECT max(PRCSNG_DT) INTO latest_prcsng_dt
      FROM DLY_BILNG WHERE MRKT_ID=p_mrkt_id AND SLS_PERD_ID in(actual_sls_perd)
      GROUP BY MRKT_ID,SLS_PERD_ID; 
    IF latest_prcsng_dt IS NOT NULL THEN 
      processing_dt:=LEAST(latest_prcsng_dt,bst_bilng_dt);
    ELSIF before2_sls_perd IS NOT NULL THEN
      SELECT max(PRCSNG_DT) INTO latest_prcsng_dt
        FROM DLY_BILNG WHERE MRKT_ID=p_mrkt_id AND SLS_PERD_ID in(before2_sls_perd)
        GROUP BY MRKT_ID,SLS_PERD_ID; 
      IF latest_prcsng_dt IS NOT NULL THEN 
        processing_dt:=LEAST(latest_prcsng_dt,est_bilng_dt);
      END IF;
    END IF;  
    RETURN processing_dt;
END;

END CASH_VLU_MANTNC;

