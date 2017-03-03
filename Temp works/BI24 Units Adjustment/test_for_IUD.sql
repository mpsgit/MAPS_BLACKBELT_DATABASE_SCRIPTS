set serveroutput on;

-- select * from table(pa_dly_bilng_adjstmnt.get_dly_bilng_adjstmnt(68,20170305,20170304,to_date('2016/07/26','YYYY/MM/DD')));
-- select * from table(pa_dly_bilng_adjstmnt.get_dly_bilng_adjstmnt2(NUMBER_ARRAY(77183180)));

                        
declare
  p_dly_bilng_id NUMBER:=77183180;
  p_new_bi24_units NUMBER:=3000;
  P_USER_ID VARCHAR2(35):='tomeoi';
  res NUMBER;
  PROCEDURE test_res(casename IN varchar, res IN number) AS
    BEGIN
      IF res=0 THEN
        DBMS_OUTPUT.PUT_LINE(casename || ' OK');
      ELSE
        DBMS_OUTPUT.PUT_LINE(casename || ' Error: ' || res );
      END IF;  
  END;
begin
  DBMS_OUTPUT.PUT_LINE(p_dly_bilng_id || ', ' || p_new_bi24_units || ', ' || P_USER_ID );
 -- delete if exists
  PA_DLY_BILNG_ADJSTMNT.SET_DLY_BILNG_ADJSTMNT(p_dly_bilng_id,p_new_bi24_units,P_USER_ID,res);
  test_res('First deletion',res);
 -- add
  p_new_bi24_units:=50;
  PA_DLY_BILNG_ADJSTMNT.SET_DLY_BILNG_ADJSTMNT(p_dly_bilng_id,p_new_bi24_units,P_USER_ID,res);
  test_res('Add',res);
 -- update
  p_new_bi24_units:=500;
  PA_DLY_BILNG_ADJSTMNT.SET_DLY_BILNG_ADJSTMNT(p_dly_bilng_id,p_new_bi24_units,P_USER_ID,res);
  test_res('Update',res);
 -- delete
  p_new_bi24_units:=NULL;
  PA_DLY_BILNG_ADJSTMNT.SET_DLY_BILNG_ADJSTMNT(p_dly_bilng_id,p_new_bi24_units,P_USER_ID,res);
  test_res('Deletion',res);
   -- add wrong
  p_new_bi24_units:=-1;
  PA_DLY_BILNG_ADJSTMNT.SET_DLY_BILNG_ADJSTMNT(p_dly_bilng_id,p_new_bi24_units,P_USER_ID,res);
  test_res('Add out of range -1',res);
   -- add
  p_new_bi24_units:=55;
  PA_DLY_BILNG_ADJSTMNT.SET_DLY_BILNG_ADJSTMNT(p_dly_bilng_id,p_new_bi24_units,P_USER_ID,res);
  test_res('Add final',res);
 END;