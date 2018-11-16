spool upgrade_dev.log

connect cedev/cedev@cedev

@upgrade11.sql

connect wedev/wedev@cedev

@upgrade11.sql

--connect mxdev/mxdev_la2q@mxdev

--@upgrade7.sql

spool off
