set serveroutput on;

-- select * from table(pa_sku_bias.get_sku_bias(68,20170305));
-- select * from table(pa_sku_bias.get_sku_bias2(68,20170305,(NUMBER_ARRAY(2139005)));

declare
  P_MRKT_ID NUMBER:=68;
  P_SLS_PERD_ID NUMBER:=20170305;
  P_SKU_ID NUMBER:=2139005;
  P_NEW_SKU_BIAS NUMBER:=100;
  P_USER_ID VARCHAR2(35):='tomeoi';
  res NUMBER;
  line OBJ_SKU_BIAS_MANTNC_LINE;
  PROCEDURE test_res(casename IN varchar, res IN number) AS
    BEGIN
      IF res=0 THEN
        DBMS_OUTPUT.PUT_LINE(casename || ' OK');
      ELSE
        DBMS_OUTPUT.PUT_LINE(casename || ' Error: ' || res );
      END IF;  
  END;
begin
  DBMS_OUTPUT.PUT_LINE(P_MRKT_ID || ', ' || P_SLS_PERD_ID || ', ' || P_SKU_ID || ', ' || P_NEW_SKU_BIAS || ', ' || P_USER_ID );
 -- delete if exists
  pa_sku_bias.SET_sku_bias(P_MRKT_ID,P_SLS_PERD_ID,P_SKU_ID,P_NEW_SKU_BIAS,P_USER_ID,res);
  test_res('First deletion',res);
 -- add
  P_NEW_SKU_BIAS:=50;
  pa_sku_bias.SET_sku_bias(P_MRKT_ID,P_SLS_PERD_ID,P_SKU_ID,P_NEW_SKU_BIAS,P_USER_ID,res);
  test_res('Add',res);
 -- update
  P_NEW_SKU_BIAS:=500;
  pa_sku_bias.SET_sku_bias(P_MRKT_ID,P_SLS_PERD_ID,P_SKU_ID,P_NEW_SKU_BIAS,P_USER_ID,res);
  test_res('Update',res);
 -- delete
  P_NEW_SKU_BIAS:=NULL;
  pa_sku_bias.SET_sku_bias(P_MRKT_ID,P_SLS_PERD_ID,P_SKU_ID,P_NEW_SKU_BIAS,P_USER_ID,res);
  test_res('Deletion',res);
   -- add wrong
  P_NEW_SKU_BIAS:=5500;
  pa_sku_bias.SET_sku_bias(P_MRKT_ID,P_SLS_PERD_ID,P_SKU_ID,P_NEW_SKU_BIAS,P_USER_ID,res);
  test_res('Add out of range 5500',res);
  P_NEW_SKU_BIAS:=-1;
  pa_sku_bias.SET_sku_bias(P_MRKT_ID,P_SLS_PERD_ID,P_SKU_ID,P_NEW_SKU_BIAS,P_USER_ID,res);
  test_res('Add out of range -1',res);
   -- add
  P_NEW_SKU_BIAS:=55;
  pa_sku_bias.SET_sku_bias(P_MRKT_ID,P_SLS_PERD_ID,P_SKU_ID,P_NEW_SKU_BIAS,P_USER_ID,res);
  test_res('Add final',res);
 END;