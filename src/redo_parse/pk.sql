select COLUMN_ID, COLUMN_NAME, DATA_TYPE from dba_tab_cols where 
owner=upper('&1') and table_name=upper('&2') 
and column_id is not null
/

select column_id 
from dba_cons_columns col, dba_constraints con, dba_tab_cols tc 
where con.owner=col.owner 
and  con.table_name = col.table_name 
and  con.CONSTRAINT_NAME= col.CONSTRAINT_NAME 
and  con.CONSTRAINT_TYPE='P' 
and  con.owner=upper('&1') 
and  con.table_name=upper('&2') 
and  con.column_name = tc.column_name 
and  tc.table_name = upper('&2') 
and  tc.owner = upper('&1')
/
