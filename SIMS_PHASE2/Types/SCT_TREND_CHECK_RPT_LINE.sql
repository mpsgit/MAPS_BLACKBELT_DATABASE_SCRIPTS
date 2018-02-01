create or replace type SCT_TREND_CHECK_RPT_LINE FORCE as object
(
  rpt_date DATE,
  fsc_code VARCHAR2(8),
  FSC_NM varchar2(100),
  bst_sc_trgt_perd_id number,
  bst_sc_sls_perd_id number,
  BST_SC_UNITS number,
  EST_SC_SLS_PERD_ID number,
  est_sc_trgt_perd_id NUMBER,
  EST_SC_UNITS number,
  NXT_EST_SC_SLS_PERD_ID number,
  nxt_est_sc_trgt_perd_id NUMBER,
  nxt_esc_sc_units NUMBER
);
/
show error

