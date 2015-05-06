drop table target;
create table target (b varchar2(4000), d varchar2(4000), e varchar2(4000), f varchar2(4000), g varchar2(4000), h varchar2(4000), a number(38,10) primary key, c date);
alter system switch logfile;

insert into target values(lpad('a', 4000, 'a'), lpad('b', 4000, 'b'), lpad('c', 4000, 'c'), lpad('d', 4000, 'd'), lpad('g', 4000, 'g'), lpad('h', 4000, 'h'), -1, sysdate);
commmit;

-- missed, since the old pk is in 11.16. current I ignore it
update target set b='b', d='d', e='c' where a = -1;
commit;

--missed also, col_no is calcuate incorrectly.  this issue should be fixed in this ticket
update target set b='b', d='d', e='c', a=1 where a = -1;
commit;
alter system switch logfile;
