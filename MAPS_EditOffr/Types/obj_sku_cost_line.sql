CREATE OR REPLACE TYPE obj_sku_cost_line FORCE AS OBJECT
(
  mrkt_id                 NUMBER,
  offr_perd_id            NUMBER,   
  sku_id                  NUMBER,   
  cost_typ                VARCHAR2(5), 
  crncy_cd                VARCHAR2(5), 
  wghtd_avg_cost_amt      NUMBER(15, 3), 
  creat_user_id           VARCHAR2(35), 
  creat_ts                DATE, 
  last_updt_user_id       VARCHAR2(35), 
  last_updt_ts            DATE, 
  hold_costs_ind          CHAR(1),
  lcl_cost_ind            CHAR(1)
);
/

CREATE OR REPLACE TYPE obj_sku_cost_table FORCE AS TABLE OF obj_sku_cost_line;
/
