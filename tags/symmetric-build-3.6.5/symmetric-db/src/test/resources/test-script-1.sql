--
-- Licensed to JumpMind Inc under one or more contributor
-- license agreements.  See the NOTICE file distributed
-- with this work for additional information regarding
-- copyright ownership.  JumpMind Inc licenses this file
-- to you under the GNU General Public License, version 3.0 (GPLv3)
-- (the "License"); you may not use this file except in compliance
-- with the License.
--
-- You should have received a copy of the GNU General Public License,
-- version 3.0 (GPLv3) along with this library; if not, see
-- <http://www.gnu.org/licenses/>.
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
--

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