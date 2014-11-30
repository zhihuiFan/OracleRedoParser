--- Purpose:  Gather all the test cases for log mining 
--- Author :  Fan Zhihui
--- Created:  2014/11/29
set echo on
drop table target;
create table target (a int primary key, b varchar2(4000), c date, d varchar2(4000), e varchar2(4000));
alter system switch logfile;

insert into target(a, b, c) values(1, 'abcdef',  sysdate);
commit;
insert into target(a, b, c)  values(2, 'abcdef',  sysdate);
commit;
update target set b='ABCDEF' where a=1;
commit;
delete from target where a=2;
delete from target where a=1;
commit;

insert into target(a, b, c)  select object_id, object_name, LAST_DDL_TIME from dba_objects where rownum < 3;
commit;

-- Mulit-Insert
-- This will not generate Mulit-Insert, row too long?
insert all
    into target(a, b, c) values(-100, 'ab', sysdate-1)
    into target(a) values(-101)
    into target(a, c) values(-102, sysdate-2)
select * from dual;
drop table test;
create table test (a int primary key, b varchar2(40), c date, d varchar2(40), e varchar2(40));
-- This will generate Mulit-Insert
insert all
   into test values(-1, 'a', sysdate, 'a', 'a')
   into test(a) values(-2)
   into test(a,c) values(-3, sysdate-1)
select * from dual;
commit;
-- all the Test passed @ d27167f93825c15629c789f464d0c534b5b6cce5


-- Row Chaining, not support so far
insert into target values(-1, lpad('a', 4000, 'a'),  sysdate, lpad('b', 4000, 'b'), lpad('c', 4000, 'c'));
commit;



alter system switch logfile;
!sleep 5
select name from (select name from v$archived_log order by sequence# desc) where rownum < 4;
