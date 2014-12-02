select position
from dba_cons_columns col, dba_constraints con
where con.owner=col.owner
and  con.table_name = col.table_name
and con.CONSTRAINT_NAME= col.CONSTRAINT_NAME 
and  con.CONSTRAINT_TYPE='P'
and  con.owner=upper('&1') 
AND CON.TABLE_NAME=upper('&2')
/
