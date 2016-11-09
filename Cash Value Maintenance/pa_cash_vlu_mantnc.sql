create or replace PACKAGE PA_CASH_VLU_MANTNC IS
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
  
END PA_CASH_VLU_MANTNC;

create or replace PACKAGE BODY PA_CASH_VLU_MANTNC AS
/*********************************************************
* History
* Created by   : Schiff Gy
* Date         : 12/10/2016
* Description  : First created 
******************************************************/

FUNCTION get_local_prcsng_dt(p_mrkt_id NUMBER, p_sls_perd NUMBER) RETURN DATE;

FUNCTION get_latest_prcsng_dt(p_mrkt_id NUMBER, p_sls_perd NUMBER) RETURN DATE;                              
  
FUNCTION GET_CASH_VLU_MANTNC(p_mrkt_id IN NUMBER,
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
                       WHERE MRKT_ID=p_mrkt_id ) M, MRKT_PERD T
      LEFT JOIN MRKT_SLS_PERD SP
        ON SP.MRKT_ID=T.MRKT_ID AND SP.SLS_PERD_ID=T.PERD_ID
      LEFT JOIN MPS_USER U
        ON U.USER_NM=SP.LAST_UPDT_USER_ID
    WHERE T.PERD_TYP='SC'
      AND T.MRKT_ID=p_mrkt_id
      AND T.PERD_ID IN((select column_value from table( p_sls_perd_list)))
    ORDER BY T.MRKT_ID, T.PERD_ID desc;
    g_run_id NUMBER       := APP_PLSQL_OUTPUT.generate_new_run_id;
    g_user_id VARCHAR(35) := USER;
    min_perd NUMBER:=p_sls_perd_list(1);
    max_perd NUMBER:=p_sls_perd_list(p_sls_perd_list.LAST);
    signature VARCHAR(100) := 'GET_CASH_VLU_MANTNC(p_mrkt_id: ' || p_mrkt_id || ', ' ;
  BEGIN
    signature := signature || 'p_sls_perd_list: '|| min_perd || '-' || max_perd || ')';
    APP_PLSQL_LOG.register(g_user_id);
    APP_PLSQL_OUTPUT.set_run_id(g_run_id);
    APP_PLSQL_LOG.set_context(g_user_id, 'PA_CASH_VLU_MANTNC', g_run_id);
    APP_PLSQL_LOG.info(signature || ' start');
    FOR rec in cc LOOP
      pipe row(rec.cline);
    END LOOP;
    APP_PLSQL_LOG.info(signature || ' stop');
  END;  

  PROCEDURE SET_CASH_VLU_MANTNC(p_mrkt_id IN NUMBER,
                                    p_sls_perd_id IN NUMBER,
                                    p_cash_val IN NUMBER,
                                    p_r_factor IN NUMBER,
                                    p_user_id IN VARCHAR2,
                                    p_stus OUT NUMBER) IS
    /*********************************************************
    * INPUT p_r_factor IS NULL handled as do nothing with this field
    *
    * Possible OUT Values
    * 0 - success
    * 1 - New CASH_VAL value is negative, or NULL
    * 2 - database error in UPDATE MRKT_SLS_PERD or INSERT INTO CASH_VAL_RF_HIST statements
    * 3 - record to update in table MRKT_SLS_PERD is readonly (has records in DLY_BILNG)
    * 4 - period to update is not defined yet in table MRKT_PERD
    ******************************************************/
    max_perd_id MRKT_SLS_PERD.SLS_PERD_ID%TYPE;
    local_prcsng_dt DATE:=NULL;                                
    old_sct_cash_val MRKT_SLS_PERD.SCT_CASH_VAL%TYPE;
    old_sct_r_factor MRKT_SLS_PERD.SCT_R_FACTOR%TYPE;
    OLD_LAST_UPDT_USER_ID MRKT_SLS_PERD.LAST_UPDT_USER_ID%TYPE;
    mrkt_perd_exists NUMBER;
    wtd CHAR(1):='N';
    ignore_r_factor BOOLEAN:=p_r_factor IS NULL;
  BEGIN
    p_stus:=0;
    IF nvl(p_cash_val,-1)<0 THEN p_STUS:=1;
    ELSE
      SELECT max(SLS_PERD_ID) into max_perd_id FROM DLY_BILNG WHERE MRKT_ID=p_mrkt_id;
      IF p_sls_perd_id > max_perd_id THEN
  -- Read current record for supplied market/campaign from MRKT_SLS_PERD
        SELECT count(*) INTO mrkt_perd_exists 
          FROM MRKT_SLS_PERD WHERE MRKT_ID=p_mrkt_id AND SLS_PERD_ID=p_sls_perd_id;
        IF mrkt_perd_exists=1 THEN
          SELECT SCT_CASH_VAL,SCT_R_FACTOR,LAST_UPDT_USER_ID 
          INTO  old_sct_cash_val,old_sct_r_factor,OLD_LAST_UPDT_USER_ID
          FROM MRKT_SLS_PERD WHERE MRKT_ID=p_mrkt_id AND SLS_PERD_ID=p_sls_perd_id;
  -- If either the Cash Value or the R-Factor is different to current values
          IF old_sct_cash_val=p_cash_val AND OLD_LAST_UPDT_USER_ID = p_user_id AND (ignore_r_factor OR old_sct_r_factor=p_r_factor)
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
          local_prcsng_dt:=null;--CASH_VLU_MANTNC.get_local_prcsng_dt(p_mrkt_id, p_sls_perd_id);
          DECLARE
            r_factor_to_set NUMBER;
          BEGIN
            IF ignore_r_factor THEN 
              r_factor_to_set:=nvl(old_sct_r_factor,NULL);
            ELSE 
              r_factor_to_set:=p_r_factor;
            END IF;
            SAVEPOINT before_changes;
  -- Upsert the appropriate record in MRKT_SLS_PERD to set new values including local processing date
            IF wtd = 'U' THEN
              UPDATE MRKT_SLS_PERD
              SET MRKT_SLS_PERD.SCT_CASH_VAL=p_cash_val,
                  MRKT_SLS_PERD.SCT_R_FACTOR=r_factor_to_set,
                  MRKT_SLS_PERD.SCT_PRCSNG_DT=local_prcsng_dt,
                  MRKT_SLS_PERD.LAST_UPDT_USER_ID=p_user_id
                WHERE MRKT_ID=p_mrkt_id AND SLS_PERD_ID=p_sls_perd_id;
            ELSE
              INSERT INTO MRKT_SLS_PERD(MRKT_ID,SLS_PERD_ID,SCT_CASH_VAL,SCT_R_FACTOR,SCT_PRCSNG_DT,CREAT_USER_ID,LAST_UPDT_USER_ID)
                VALUES (p_mrkt_id,p_sls_perd_id,p_cash_val,r_factor_to_set,local_prcsng_dt,p_user_id,p_user_id);
            END IF;
  -- Insert a record into CASH_VAL_RF_HIST to record the previous values
            INSERT INTO CASH_VAL_RF_HIST(MRKT_ID,SLS_PERD_ID,CASH_VAL,R_FACTOR,PRCSNG_DT,LAST_UPDT_USER_ID)
              SELECT MRKT_ID,SLS_PERD_ID,p_cash_val,r_factor_to_set,local_prcsng_dt,p_user_id 
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
      latest_prcsng_dt:=get_latest_prcsng_dt(p_mrkt_id,actual_sls_perd);
      IF latest_prcsng_dt IS NOT NULL THEN 
        processing_dt:=LEAST(latest_prcsng_dt,bst_bilng_dt);
      ELSIF before2_sls_perd IS NOT NULL THEN
        latest_prcsng_dt:=get_latest_prcsng_dt(p_mrkt_id,before2_sls_perd);
        IF latest_prcsng_dt IS NOT NULL THEN 
          processing_dt:=LEAST(latest_prcsng_dt,est_bilng_dt);
        END IF;
      END IF;  
      RETURN processing_dt;
  END;
  
  FUNCTION get_latest_prcsng_dt(p_mrkt_id NUMBER, p_sls_perd NUMBER) RETURN DATE IS
    latest_prcsng_dt date;
  BEGIN
    SELECT max(PRCSNG_DT) INTO latest_prcsng_dt
      FROM DLY_BILNG WHERE MRKT_ID=p_mrkt_id AND SLS_PERD_ID=p_sls_perd
      GROUP BY MRKT_ID,SLS_PERD_ID;
    RETURN latest_prcsng_dt; 
  EXCEPTION WHEN NO_DATA_FOUND THEN RETURN NULL;
  END;
  
END PA_CASH_VLU_MANTNC;