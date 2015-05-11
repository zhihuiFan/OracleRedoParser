drop table target;
create table target (b varchar2(4000), d varchar2(4000), e varchar2(4000), f varchar2(4000), g varchar2(4000), h varchar2(4000), a number(38,10) , c date, primary key(a, b));
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
commit;

alter system switch logfile;
