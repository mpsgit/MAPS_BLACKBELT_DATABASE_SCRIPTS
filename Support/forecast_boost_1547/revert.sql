PROMPT
PROMPT Reverting Support_Issue_1547
PROMPT

set feedback off termout off pagesize 0
col log_file new_value log_file
select user||'_'||instance_name||'_'||to_char(sysdate, 'yyyy-mm-dd')||'_revert_support_issue_1547.log' log_file from v$instance;
set feedback on termout on

spool '&log_file'

set serveroutput on size 1000000 format wrapped
set head off
select user || '@' || instance_name || ' on ' || host_name from v$instance;
select ' Start: ' || to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') from dual;
set head on

-- revert
-- tables, indexes, triggers          (6 -Revert)
@@"create_table/6 -Revert/drop_new_tables.sql"
-- Copy_Offer     (revert to previous version)
@@"Copy_Offer/revert_cpy_offr.sql"

set head off
select user || '@' || instance_name || ' on ' || host_name from v$instance;
select ' End:   ' || to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') from dual;
set head on

PROMPT Reverted Support_Issue_1547
spool off
