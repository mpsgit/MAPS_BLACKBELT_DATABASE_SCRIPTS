spool upgrade_stg.log

connect cestg/cestg@cedev

@upgrade11.sql

connect westg/westg@cedev

@upgrade11.sql

connect anstg/anstg_la2q@lamaps2

@upgrade11.sql

connect brstg/brstg_la2q@lamaps2

@upgrade11.sql

connect cnstg/cnstg01@apmaps2

@upgrade11.sql

connect mxstg/mxstg_la2q@lamaps2

@upgrade11.sql

connect scstg/scstg_la2q@lamaps2

@upgrade11.sql

connect sestg/sestg01@apmaps2

@upgrade11.sql

spool off
