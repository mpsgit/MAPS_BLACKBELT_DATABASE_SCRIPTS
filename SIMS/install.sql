PROMPT
PROMPT Installing Release_20170502
PROMPT

set feedback off termout off pagesize 0
col log_file new_value log_file
select user||'_'||instance_name||'_2017.05.02_install_'||to_char(sysdate, 'yyyymmdd')||'.log' log_file from v$instance;
set feedback on termout on

spool '&log_file'

set serveroutput on size 1000000 format wrapped
set head off
select user || '@' || instance_name || ' on ' || host_name from v$instance;
select ' Start: ' || to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') from dual;
set head on

-- types  (./)
@@"table_types.sql"
-- tables, indexes, triggers          (1 -New tables)
@@"create_table/1 -New tables/c_bi24_genrtn_statss.sql"
@@"create_table/1 -New tables/c_cash_val_rf_hist.sql"
@@"create_table/1 -New tables/c_dly_bilng_adjstmnt.sql"
@@"create_table/1 -New tables/c_mrkt_perd_sku_bias.sql"
@@"create_table/1 -New tables/c_sct_fsc_ovrrd.sql"
@@"create_table/1 -New tables/c_trend_offst.sql"
@@"create_table/1 -New tables/c_ta_dict.sql"
@@"create_table/1 -New tables/c_ta_config.sql"
@@"create_table/1 -New tables/c_trend_alloc_hist_dtls.sql"
@@"create_table/1 -New tables/c_trend_alloc_hist_dtls_log.sql"
@@"create_table/1 -New tables/c_mrkt_trnd_sls_perd.sql"
@@"create_table/1 -New tables/c_mrkt_trnd_sls_perd_sls_typ.sql"
@@"create_table/1 -New tables/c_custm_rul_mstr.sql"
@@"create_table/1 -New tables/c_custm_rul_perd.sql"
@@"create_table/1 -New tables/c_custm_rul_sku_list.sql"
@@"create_table/1 -New tables/c_custm_seg_mstr.sql"
@@"create_table/1 -New tables/c_custm_seg_perd.sql"
@@"create_table/1 -New tables/i_bi24_genrtn_statss.sql"
@@"create_table/1 -New tables/i_cash_val_rf_hist.sql"
@@"create_table/1 -New tables/i_dly_bilng_adjstmnt.sql"
@@"create_table/1 -New tables/i_fsc_sct_ovrrd.sql"
@@"create_table/1 -New tables/i_mrkt_perd_sku_bias.sql"
@@"create_table/1 -New tables/i_trend_offst.sql"
@@"create_table/1 -New tables/tr_cash_val_rf_hist.sql"
@@"create_table/1 -New tables/tr_dly_bilng_adjstmnt.sql"
@@"create_table/1 -New tables/tr_mrkt_perd_sku_bias.sql"
@@"create_table/1 -New tables/tr_sct_fsc_ovrrd.sql"
@@"create_table/1 -New tables/tr_trend_offst.sql"
@@"create_table/1 -New tables/tr_mrkt_trnd_sls_perd.sql"
@@"create_table/1 -New tables/tr_mrkt_trnd_sls_perd_sls_typ.sql"
@@"create_table/1 -New tables/tr_custmseg_mstr.sql"
@@"create_table/1 -New tables/tr_custmseg_perd.sql"
@@"create_table/1 -New tables/tr_custm_rul_mstr.sql"
@@"create_table/1 -New tables/tr_custm_rul_perd.sql"
@@"create_table/1 -New tables/tr_custm_rul_skulst.sql"
-- alter tables, indexes              (2 -Altered tables)
@@"create_table/2 -Altered tables/i_mrkt_sku_additional.sql"
-- data input    (3 -Data Input)
@@"create_table/3 -Data Input/initiate_trend_offst.sql"
@@"create_table/3 -Data Input/maintain_config_item___mrkt_config_item.sql"
@@"create_table/3 -Data Input/initiate_ta_dict.sql"
@@"create_table/3 -Data Input/initiate_ta_config.sql"
-- SIMS Maintenance    (SIMS Maintenance)
@@"SIMS Maintenance/pa_cash_vlu_mantnc.sql"
@@"SIMS Maintenance/pa_dly_bilng_adjstmnt.sql"
@@"SIMS Maintenance/pa_manl_trend_adjstmnt.sql"
@@"SIMS Maintenance/pa_sku_bias.sql"
@@"SIMS Maintenance/pa_sims_cust_grp_mantnc.sql"
@@"SIMS Maintenance/pa_sims_mantnc.sql"
-- Trend Allocation    (Trend Allocation)
@@"Trend Allocation/pa_trend_alloc.sql"
-- JOB for Trend Allocation
@@"Trend Allocation/cj_pa_trend_alloc_job.sql"

set head off
select user || '@' || instance_name || ' on ' || host_name from v$instance;
select ' End:   ' || to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') from dual;
set head on

PROMPT Installed Release_20170502
spool off
