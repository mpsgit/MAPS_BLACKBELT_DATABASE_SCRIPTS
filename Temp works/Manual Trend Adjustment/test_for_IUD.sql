set serveroutput on;

-- select * from table(pa_manl_trend_adjstmnt.get_manl_trend_adjstmnt(68,20170305,4));
-- select * from table(pa_manl_trend_adjstmnt.get_manl_trend_adjstmnt2(68,20170305,4,NUMBER_ARRAY(74958)));

declare
  p_mrkt_id NUMBER:=68;
  p_sls_perd_id NUMBER:=20170305;
  p_sls_typ_id NUMBER:=4;
  p_fsc_cd NUMBER:=74958;
  p_sct_unit_qty NUMBER:=3000;
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
  DBMS_OUTPUT.PUT_LINE(p_mrkt_id || ', ' || p_sls_perd_id || ', ' || p_sls_typ_id || ', ' || p_fsc_cd || ', ' || p_sct_unit_qty || ', ' || P_USER_ID );
 -- delete if exists
  PA_MANL_TREND_ADJSTMNT.SET_MANL_TREND_ADJSTMNT(p_mrkt_id, p_sls_perd_id, p_sls_typ_id,
                                  p_fsc_cd, p_sct_unit_qty, p_user_id, res);
  test_res('First deletion',res);
 -- add
  p_sct_unit_qty:=50;
  PA_MANL_TREND_ADJSTMNT.SET_MANL_TREND_ADJSTMNT(p_mrkt_id, p_sls_perd_id, p_sls_typ_id,
                                  p_fsc_cd, p_sct_unit_qty, p_user_id, res);
  test_res('Add',res);
 -- update
  p_sct_unit_qty:=500;
  PA_MANL_TREND_ADJSTMNT.SET_MANL_TREND_ADJSTMNT(p_mrkt_id, p_sls_perd_id, p_sls_typ_id,
                                  p_fsc_cd, p_sct_unit_qty, p_user_id, res);
  test_res('Update',res);
 -- delete
  p_sct_unit_qty:=NULL;
  PA_MANL_TREND_ADJSTMNT.SET_MANL_TREND_ADJSTMNT(p_mrkt_id, p_sls_perd_id, p_sls_typ_id,
                                  p_fsc_cd, p_sct_unit_qty, p_user_id, res);
  test_res('Deletion',res);
   -- add wrong
  p_sct_unit_qty:=-1;
  PA_MANL_TREND_ADJSTMNT.SET_MANL_TREND_ADJSTMNT(p_mrkt_id, p_sls_perd_id, p_sls_typ_id,
                                  p_fsc_cd, p_sct_unit_qty, p_user_id, res);
  test_res('Add out of range -1',res);
   -- add
  p_sct_unit_qty:=55;
  PA_MANL_TREND_ADJSTMNT.SET_MANL_TREND_ADJSTMNT(p_mrkt_id, p_sls_perd_id, p_sls_typ_id,
                                  p_fsc_cd, p_sct_unit_qty, p_user_id, res);
  test_res('Add final',res);
 END;