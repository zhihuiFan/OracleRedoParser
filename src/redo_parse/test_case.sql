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

insert into target(a, b, c)  select object_id, object_name, LAST_DDL_TIME from dba_objects where rownum < 8;
commit;
alter system switch logfile;
!sleep 5
select name from (select name from v$archived_log order by sequence# desc) where rownum < 4;
-- all the Test passed @ d27167f93825c15629c789f464d0c534b5b6cce5


-- Row Chaining
insert into target values(-1, lpad('a', 4000, 'a'),  sysdate, lpad('b', 4000, 'b'), lpad('c', 4000, 'c'));
commit;
