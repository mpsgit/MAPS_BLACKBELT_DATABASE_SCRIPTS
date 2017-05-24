PROMPT
PROMPT Installing Support_Issue_1547
PROMPT

set feedback off termout off pagesize 0
col log_file new_value log_file
select user||'_'||instance_name||'_'||to_char(sysdate, 'yyyy-mm-dd')||'_install_support_issue_1547.log' log_file from v$instance;
set feedback on termout on

spool '&log_file'

set serveroutput on size 1000000 format wrapped
set head off
select user || '@' || instance_name || ' on ' || host_name from v$instance;
select ' Start: ' || to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') from dual;
set head on

-- tables, indexes, triggers          (1 -New tables)
@@"create_table/1 -New tables/c_frcst_boost_xclusn_mrkt_perd.sql"
@@"create_table/1 -New tables/tr_frcst_boost_xclusn_mrkt_perd.sql"
-- Copy_Offer
@@"Copy_Offer/cpy_offr.sql"

set head off
select user || '@' || instance_name || ' on ' || host_name from v$instance;
select ' End:   ' || to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') from dual;
set head on

PROMPT Installed Support_Issue_1547
spool off
