--- Purpose:  Gather all the test cases for log mining 
--- Author :  Fan Zhihui
--- Created:  2014/11/29
conn andy/andy
set echo on
drop table target;
create table target (b varchar2(4000), a number(38,10) primary key, c date, d varchar2(4000), e varchar2(4000));
drop table test;
create table test (a int primary key, b varchar2(40), c date, d varchar2(40), e varchar2(40));

alter system switch logfile;

-- insert part of columns
whenever sqlerror exit
insert into target(a, b, c) values(1, 'FANZHIHUI',  sysdate);
commit;
insert into target(a, b, c)  values(2, 'abcdef',  sysdate);
commit;
update target set b='ABCDEF' where a=1;
commit;
-- delete rows, which only parts of column have value
delete from target where a=2;
delete from target where a=1;
commit;

insert into target(a, b, c)  select object_id, object_name, LAST_DDL_TIME from dba_objects where rownum < 3;
commit;

-- Mulit-Insert
-- This will not generate Mulit-Insert, row too long?
insert all
    into target(a, b, c) values(-100, 'ab', sysdate-1)
    into target(a, c) values(-102.22, sysdate-2)
    into target(a, b, c) values(-0.0101, 'ab', sysdate-1)
    into target(a, b, c) values(100, 'ab', sysdate-1)
    into target(a, c) values(102.22, sysdate-2)
    into target(a, b, c) values(0.0101, 'ab', sysdate-1)
    into target(a) values(0)
select * from dual;
commit;
delete from target where a=-100;
delete from target where a=-101;
rollback;
-- This will generate Mulit-Insert
insert all
   into test values(-1, 'a', sysdate, 'a', 'a')
   into test(a) values(-2)
   into test(a,c) values(-3, sysdate-1)
select * from dual;
commit;
-- all the Test passed @ 9e29ca333d14c799af32ed4d5a69e64449037822


-- Row Chaining
insert into target values(lpad('a', 4000, 'a'), -1, sysdate, lpad('b', 4000, 'b'), lpad('c', 4000, 'c'));
commit;
-- All the above test are passwd @ 516af28076bfb59e1d3bd415492fcc26606b9501
-- Row Migration
insert into target(a) values(623);
update target set b=lpad('9',4000,'9'), d=lpad('1', 4000, '1'), e=lpad('2', 4000, '2') where a=623;

---
--- 5.2 -> 5.1 -> 11.2 (pk only)
--- 5.1 -> 11.2 (col_e only)
--- 5.1 -> 11.2 (col_d only)
--- 5.1 -> 11.6 (col_b only) 11.6 is row chained
--- if row_chain is must:  ==> 11.2 ==> (0,e)
---                       pk is first, take 1 11.2

commit;

alter system switch logfile;
!sleep 5
select name from (select name from v$archived_log order by sequence# desc) where rownum < 4;
