col sql_redo format a150
set lin 255
select to_char(XIDUSN,'xxxx')||to_char(XIDSLT,'xxxx') ||to_char(XIDSQN, 'xxxxxxxx') as xid,
    to_char(scn, 'xxxxxxxxx') as scn,
    to_char(start_scn, 'xxxxxxxxx') as start_scn ,
    to_char(commit_scn, 'xxxxxxxxx') as commit_scn,
    sql_redo
   sql_redo
from V$LOGMNR_CONTENTS
where xid='&1.';
