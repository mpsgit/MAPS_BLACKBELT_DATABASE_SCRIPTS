PROMPT
PROMPT Reverting Release_20170502
PROMPT

set feedback off termout off pagesize 0
col log_file new_value log_file
select user||'_'||instance_name||'_2017.05.02_revert_'||to_char(sysdate, 'yyyymmdd')||'.log' log_file from v$instance;
set feedback on termout on

spool '&log_file'

set serveroutput on size 1000000 format wrapped
set head off
select user || '@' || instance_name || ' on ' || host_name from v$instance;
select ' Start: ' || to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') from dual;
set head on

-- revert
-- tables, indexes, triggers          (6 -Revert)
@@"create_table/6 -Revert/i_mrkt_sku_additional_revert.sql"
@@"create_table/6 -Revert/drop_new_tables.sql"
-- types   (./)
@@"table_type_reverts.sql"
-- SIMS Maintenance (revert to ???)
-- Trend Allocation (revert to ???)
-- JOB for Trend Allocation
@@"Trend Allocation/cj_pa_trend_alloc_job_revert.sql"

set head off
select user || '@' || instance_name || ' on ' || host_name from v$instance;
select ' End:   ' || to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') from dual;
set head on

PROMPT Reverted Release_20170502
spool off
