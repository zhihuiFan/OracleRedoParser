--- Purpose:  Gather all the test cases for log mining 
--- Created:  2014/11/29
conn andy/andy
create tablespace tbs_5blocks datafile '/home/oracle/app/oracle/oradata/momo/tbs01.dbf' size 10m uniform size 40k segment space management manual;
drop table target;
drop table t1106;

-- setup target table
drop table halv.target;
create table halv.target (xid number, scn number, op varchar2(40), b varchar2(4000), a number(38,10), c date);
drop table halv.t1106;
create table halv.t1106(xid number, scn number, op varchar2(40), id int);

whenever sqlerror exit
set echo on
create table target (b varchar2(4000), d varchar2(4000), e varchar2(4000), f varchar2(4000), g varchar2(4000), h varchar2(4000), a number(38,10) , c date, primary key(a, b, c));

create table t1106(id int primary key, c1 varchar2(2000), c2 varchar2(2000), c3 varchar2(2000), c4 varchar2(2000) ) tablespace tbs_5blocks;
truncate table t1106;
begin
     for i in 1..63 loop
       insert into t1106 values(i,rpad('A',100,'A'),rpad('B',100,'B'),rpad('C',100,'C'),rpad('D',100,'D'));
     end loop;
     commit;
end;
/

alter system switch logfile;

-- Big Insert, PK located in 2 different blocks
insert into target values(lpad('a', 4000, 'a'), lpad('b', 4000, 'b'), lpad('c', 4000, 'c'), lpad('d', 4000, 'd'), lpad('g', 4000, 'g'), lpad('h', 4000, 'h'), -1, sysdate);
commit;

--- Big Update with touching  1 primary key
update target set b='b', d='d', e='c' where a = -1;
commit;

--- Big Update with touching  2 primary key
update target set b='b', d='d', e='c', a=1 where a = -1;
commit;

--- Big delete
delete from target where a=1;
commit;

-- for Row megiration
-- insert into target values('a', 4000, 'a'), lpad('b', 4000, 'b'), lpad('c', 4000, 'c'), lpad('d', 4000, 'd'), lpad('g', 4000, 'g'), lpad('h', 4000, 'h'), -1, sysdate);
insert into target values('a', 'b', 'c', 'd', 'g', 'h', -1, sysdate);
commit;

-- Update Row megiration touch 1 PK.  We need to parse 11.16 and 11.6 correctly for this case
update target set b=lpad('b', 4000, 'b'), d=lpad('d', 4000, 'd'), f=lpad('f', 4000, 'f') where a = -1;

-- 11.6 + 11.3
-- Row Megiration
update t1106 set c1=rpad('A', 500,'A'), c2=rpad('B', 500,'B'), c3=rpad('C', 500,'C') where id=20;
commit;

-- Make room for row 20
delete t1106 where id between 18 and 19;
delete t1106 where id between 21 and 34;
commit;

-- Let row 20 megirate back
update t1106 set c1=rpad('A', 1000,'A'), c2=rpad('B', 2000,'B'), c3=rpad('C', 2000,'C'), id=-100 where id=20;
commit;

--- Data Tyep Test
insert into target(a,b,c ) values(0.0001, 'abdef', sysdate);
insert into target(a,b,c ) values(-0.0001, 'abdef', sysdate);
insert into target(a,b,c ) values(-1000, 'abdef', sysdate);
-- for https://jirap.corp.ebay.com/browse/DBISTREA-54
alter system switch logfile;
insert into target(a,b,c ) values(-1000.001, 'abdef', sysdate);
update target set b = 'A' , c= sysdate where a= 0.0001;
commit;

alter system switch logfile;
