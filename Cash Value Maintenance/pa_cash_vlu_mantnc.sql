create or replace PACKAGE CASH_VLU_MANTNC IS 
  /*********************************************************
  * History
  * Created by   : Schiff Gy
  * Date         : 12/10/2016
  * Description  : Update cash_value and r_factor
  ******************************************************/
  
  TYPE OBJ_CASH_VAL_MANTNC_LINE IS RECORD
   ( PERD_ID number(8),
     SCT_CASH_VAL number(15,2),
     SCT_R_FACTOR number(6,4)
     );
  TYPE OBJ_CASH_VAL_MANTNC_TABLE IS TABLE OF OBJ_CASH_VAL_MANTNC_LINE index by binary_integer;

  PROCEDURE CASH_VLU_MANTNC_INSERT(p_mrkt_id IN NUMBER, p_parm IN OBJ_CASH_VAL_MANTNC_TABLE, p_stus OUT NUMBER);
  
  
END CASH_VLU_MANTNC;

create or replace PACKAGE BODY CASH_VLU_MANTNC AS
  /*********************************************************
  * History
  * Created by   : Schiff Gy
  * Date         : 12/10/2016
  * Description  : First created 
  ******************************************************/

PROCEDURE CASH_VLU_MANTNC_INSERT(p_mrkt_id IN NUMBER, p_parm IN OBJ_CASH_VAL_MANTNC_TABLE, p_stus OUT NUMBER) IS
  /*********************************************************
  * Possible OUT Values
  * 0 - success
  * 1 - record to update in table MRKT_SLS_PERD doesn't exist (semantic error in parameters)
  * 2 - record to update in table MRKT_SLS_PERD shall be readonly (semantic error in parameters)
  * 3 - unidentified database error
  ******************************************************/
  
  max_perd_id MRKT_SLS_PERD.SLS_PERD_ID%TYPE;
  old_sct_cash_val MRKT_SLS_PERD.SCT_CASH_VAL%TYPE;
  old_sct_r_factor MRKT_SLS_PERD.SCT_R_FACTOR%TYPE;
  
  BEGIN
    p_stus:=1;
    SELECT MAX(SLS_PERD_ID) INTO max_perd_id
      FROM DLY_BILNG WHERE MRKT_ID=p_mrkt_id
      GROUP BY MRKT_ID;
    FOR i IN 1..p_parm.count LOOP
      IF p_parm(i).PERD_ID > max_perd_id-2 THEN
        BEGIN
          SELECT SCT_CASH_VAL,SCT_R_FACTOR INTO old_sct_cash_val,old_sct_r_factor
            FROM MRKT_SLS_PERD WHERE MRKT_ID=p_mrkt_id AND SLS_PERD_ID=p_parm(i).PERD_ID;
          IF old_sct_cash_val= p_parm(i).SCT_CASH_VAL AND old_sct_r_factor= p_parm(i).SCT_R_FACTOR THEN NULL;
          ELSE
            BEGIN
            SAVEPOINT before_changes;
              UPDATE MRKT_SLS_PERD
                SET MRKT_SLS_PERD.SCT_CASH_VAL=p_parm(i).SCT_CASH_VAL,
                    MRKT_SLS_PERD.SCT_R_FACTOR=p_parm(i).SCT_R_FACTOR
                WHERE MRKT_ID=p_mrkt_id AND SLS_PERD_ID=p_parm(i).PERD_ID;
              INSERT INTO CASH_VAL_RF_HIST(MRKT_ID,SLS_PERD_ID,CASH_VAL,R_FACTOR,PRCSNG_DT,LAST_UPDT_USER_ID,LAST_UPDT_TS)
                SELECT MRKT_ID,SLS_PERD_ID,nvl(old_sct_cash_val,0),old_sct_r_factor,SCT_PRCSNG_DT,LAST_UPDT_USER_ID,LAST_UPDT_TS
                  FROM MRKT_SLS_PERD WHERE MRKT_ID=p_mrkt_id AND SLS_PERD_ID=p_parm(i).PERD_ID;
            EXCEPTION WHEN NO_DATA_FOUND THEN p_stus:=1;
                      WHEN OTHERS THEN ROLLBACK TO before_changes; p_stus:=3;
            END;
          END IF;
        END;            
      ELSE p_stus:=2;
      END IF;
    END LOOP;
  END;
END;