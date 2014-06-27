-- This is a comment
select * from 
  TEST where
  -- a = 'b' and
  id = 'someid'; ## some note 
select * from test;
insert into test (one, two, three) values('1','1','2');
// Another Comment;
delete from test where one='1';
delete from test where one='1';
delete from test where one='1';
delete from test where one='1';
update sym_node set sync_url='http://localhost:8080/test' where node_id='test';
update something set oops=';' where whoops='test';
update test set one = '''', two='\\##--''' where one is null; // comment
update test
  set one = '1', two = '2'
  where one = 'one';
## last comment