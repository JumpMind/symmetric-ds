insert into sym_channel (channel_id, processing_order, max_batch_size, max_batch_to_send, contains_big_lob, enabled, description) values('testchannel', 1, 50, 50, 1, 1, null);
insert into sym_channel (channel_id, processing_order, max_batch_size, max_batch_to_send, contains_big_lob, enabled, description) values('other', 0, 1, 50, 1, 1, null);
insert into sym_node_group values ('symmetric','a group representing symmetric configuration');
insert into sym_node_group values ('test-root-group','a test config');
insert into sym_node_group values ('test-node-group','a test config');
insert into sym_node_group values ('test-node-group2','another test config');
insert into sym_node_group values ('test-node-group3','another test config');
insert into sym_node_group values ('test-group-for-root-to-pull','another test config');
insert into sym_node_group values ('unit-test-only','a group used for unit testing');
insert into sym_node_group_link values ('test-root-group','test-root-group', 'P');
insert into sym_node_group_link values ('test-root-group','test-node-group2', 'P');
insert into sym_node_group_link values ('test-root-group','test-node-group', 'W');
insert into sym_node_group_link values ('test-root-group','test-node-group3', 'W');
insert into sym_node_group_link values ('test-group-for-root-to-pull','test-root-group', 'W');
insert into sym_node_group_link values ('test-node-group','test-root-group', 'P');
insert into sym_node_group_link values ('symmetric','test-root-group', 'P');
insert into sym_node values ('00000', 'test-root-group', '00000', 1, 'internal://root', '1', '2.0','H2', '1.1', current_timestamp, null, 0, 0, '00000', 'engine');
insert into sym_node values ('1', 'test-node-group', '1', 1, 'internal://root', '1', '2.0','H2', '5.0', current_timestamp, null, 0, 0, '00000', 'engine');
insert into sym_node values ('00001', 'test-node-group', '00001', 1, 'http://localhost:8080/sync', '1', '2.0', 'H2', '5.0', current_timestamp, null, 0, 0, '00000', 'engine');
insert into sym_node values ('00002', 'test-node-group', '00002', 0, null, null, '2.0', null, null, current_timestamp, null, 0, 0, '00000', 'engine');
insert into sym_node values ('00003', 'test-node-group', '00003', 1, 'http://localhost:8080/', '0', '2.0', 'H2', '4', current_timestamp, null, 0, 0, '00000', 'engine');
insert into sym_node values ('00010', 'test-node-group2', '00010', 1, null, null, '2.0', null, null, current_timestamp, null, 0, 0, '00000', 'engine');
insert into sym_node values ('00030', 'test-node-group3', '00030', 1, null, null, '2.0', null, null, current_timestamp, null, 0, 0, '00000', 'engine');
insert into sym_node values ('00011', 'test-node-group2', '00011', 1, null, null, '2.0', null, null, current_timestamp, null, 0, 0, '00000', 'engine');
insert into sym_node values ('pull1', 'test-group-for-root-to-pull', 'test', 1, 'http://localhost:8080/sync', '1', '2.0', 'H2', '5.0', current_timestamp, null, 0, 0, '00000', 'engine');
insert into sym_node values ('pull2', 'test-group-for-root-to-pull', 'test', 1, null, null, '2.0', null, null, current_timestamp, null, 0, 0, '00000', 'engine');
insert into sym_node values ('pull3', 'test-group-for-root-to-pull', 'test', 1, 'http://localhost:8080/', '0', '2.0', 'H2', '4', current_timestamp, null, 0, 0, '00000', 'engine');
insert into sym_node values ('55555', 'test-node-group2', '00011', 1, 'http://snoopdog.com', null, '2.0', null, null, current_timestamp, null, 0, 0, '00000', 'engine');

-- For testCheckForOfflineNodes.  These nodes have an old heartbeat and will be set to offline
insert into sym_node values ('66666', 'unit-test-only', '66666', 1, null, null, '2.0', null, null, {ts '2000-01-01 00:00:00'}, null, 0, 0, '00000', 'engine');
insert into sym_node values ('77777', 'unit-test-only', '77777', 1, null, null, '2.0', null, null, {ts '2000-01-01 00:00:00'}, '-08:00', 0, 0, '00000', 'engine');

-- For testFindNodesThatOriginatedHere
insert into sym_node values ('44001', 'unit-test-only', '44001', 1, null, null, '2.0', null, null, current_timestamp, null, 0, 0, '00011', 'engine');
insert into sym_node values ('44002', 'unit-test-only', '44001', 1, null, null, '2.0', null, null, current_timestamp, null, 0, 0, '00012', 'engine');
insert into sym_node values ('44003', 'unit-test-only', '44001', 1, null, null, '2.0', null, null, current_timestamp, null, 0, 0, '00011', 'engine');
insert into sym_node values ('44004', 'unit-test-only', '44001', 1, null, null, '2.0', null, null, current_timestamp, null, 0, 0, '44002', 'engine');
insert into sym_node values ('44005', 'unit-test-only', '44001', 1, null, null, '2.0', null, null, current_timestamp, null, 0, 0, '44001', 'engine');
insert into sym_node values ('44006', 'unit-test-only', '44001', 1, null, null, '2.0', null, null, current_timestamp, null, 0, 0, '44003', 'engine');

insert into sym_node_security values ('00001', 'secret', 0, {ts '2007-01-01 01:01:01'}, 0, {ts '2007-01-01 01:01:01'}, '00000');
insert into sym_node_security values ('00002', 'supersecret', 1, null, 0, null, '00000');
insert into sym_node_security values ('00003', 'notsecret', 0, {ts '2007-01-01 01:01:01'}, 0, {ts '2007-01-01 01:01:01'}, '00000');
insert into sym_node_security values ('00000', 'notsecret', 0, {ts '2007-01-01 01:01:01'}, 0, null, '00000');
insert into sym_node_identity values ('00000');

insert into sym_router  (router_id,source_node_group_id, target_node_group_id,       create_time,  last_update_time) 
                  values(   '1000',   'test-root-group',    'test-root-group', current_timestamp, current_timestamp);
insert into sym_router  (router_id,source_node_group_id, target_node_group_id,       create_time,  last_update_time) 
                  values(   '2000',         'symmetric',    'test-root-group', current_timestamp, current_timestamp);
insert into sym_router  (router_id,source_node_group_id, target_node_group_id,       create_time,  last_update_time) 
                  values(   '3000',   'test-root-group',    'test-node-group2', current_timestamp, current_timestamp);     
insert into sym_router  (router_id,source_node_group_id, target_node_group_id,       create_time,  last_update_time) 
                  values(   'test_2_root',   'test-node-group', 'test-root-group', current_timestamp, current_timestamp);     
                  
insert into sym_trigger        (trigger_id,                source_table_name,    channel_id,                   name_for_insert_trigger,  last_update_time,      create_time)
                         values(    '1000',            'test_triggers_table', 'testchannel',                     'insert_test_tbl_trg', current_timestamp,current_timestamp);
insert into sym_trigger_router (trigger_id, router_id, initial_load_order,  last_update_time,       create_time)
                         values(    '1000',    '1000',                  1, current_timestamp, current_timestamp);
                                                                          
insert into sym_trigger        (trigger_id,                source_table_name,    channel_id,  last_update_time,      create_time)
                         values(    '2000',                 'sym_node_group', 'testchannel', current_timestamp,current_timestamp);
insert into sym_trigger_router (trigger_id, router_id, initial_load_order,  last_update_time,       create_time)
                         values(    '2000',    '2000',                  1, current_timestamp, current_timestamp);   

insert into sym_trigger        (trigger_id,                source_table_name,    channel_id,  last_update_time,      create_time)
                         values(    '3000',       'test_sync_incoming_batch', 'testchannel', current_timestamp,current_timestamp);
insert into sym_trigger_router (trigger_id, router_id, initial_load_order,  last_update_time,       create_time)
                         values(    '3000',    '3000',                  1, current_timestamp, current_timestamp);   

-- AdditiveDataLoaderFilter test data
insert into test_add_dl_table_1 values('k3','k4',1,2.0,3,4.0,5,'6',7);
insert into test_add_dl_table_1 values('k5','k6',1,3.0,5,7.0,9,'11',13);
insert into test_add_dl_table_2 values('k3',1);

insert into sym_transform_table (transform_id, source_node_group_id, target_node_group_id, source_table_name, target_table_name, transform_point, update_first, delete_action, transform_order)
  values ('simple_2_a', 'test-node-group', 'test-root-group', 'SIMPLE', 'TEST_TRANSFORM_A', 'LOAD', 0, 'DEL_ROW', 1);
insert into sym_transform_column (transform_id, include_on, source_column_name, target_column_name, pk, transform_type, transform_expression, transform_order)
  values ('simple_2_a', '*', 'ID', 'ID_A', 1, null, null, 1);  
insert into sym_transform_column (transform_id, include_on, source_column_name, target_column_name, pk, transform_type, transform_expression, transform_order)
  values ('simple_2_a', '*', 'S1', 'S1_A', 0, null, null, 2);
insert into sym_transform_column (transform_id, include_on, source_column_name, target_column_name, pk, transform_type, transform_expression, transform_order)
  values ('simple_2_a', '*', null, 'S2_A', 0,  'const', 'CONSTANT', 3);    
insert into sym_transform_column (transform_id, include_on, source_column_name, target_column_name, pk, transform_type, transform_expression, transform_order)
  values ('simple_2_a', '*', 'TOTAL', 'DECIMAL_A', 0, 'additive', null, 4);   
insert into sym_transform_column (transform_id, include_on, source_column_name, target_column_name, pk, transform_type, transform_expression, transform_order)
  values ('simple_2_a', '*', null, 'LONGSTRING_A', 0, 'bsh', 'S1+"-"+ID', 5);    
  
insert into sym_transform_table (transform_id, source_node_group_id, target_node_group_id, source_table_name, target_table_name, transform_point, update_first, delete_action, transform_order)
  values ('source1_to_a', 'test-node-group', 'test-root-group', 'SOURCE_1', 'TEST_TRANSFORM_A', 'LOAD', 0, 'DEL_ROW', 1);
insert into sym_transform_column (transform_id, include_on, source_column_name, target_column_name, pk, transform_type, transform_expression, transform_order)
  values ('source1_to_a', '*', 'ID', 'ID_A', 1, null, null, 1);  
insert into sym_transform_column (transform_id, include_on, source_column_name, target_column_name, pk, transform_type, transform_expression, transform_order)
  values ('source1_to_a', '*', 'S1', 'S1_A', 0, null, null, 2);
insert into sym_transform_column (transform_id, include_on, source_column_name, target_column_name, pk, transform_type, transform_expression, transform_order)
  values ('source1_to_a', 'I', null, 'S2_A', 0, 'const', 'CONSTANT', 3);      
  
insert into sym_transform_table (transform_id, source_node_group_id, target_node_group_id, source_table_name, target_table_name, transform_point, update_first, delete_action, transform_order)
  values ('source2_to_a', 'test-node-group', 'test-root-group', 'SOURCE_2', 'TEST_TRANSFORM_A', 'LOAD', 1, 'UPDATE_COL', 1);
insert into sym_transform_column (transform_id, include_on, source_column_name, target_column_name, pk, transform_type, transform_expression, transform_order)
  values ('source2_to_a', '*', 'ID2', 'ID_A', 1, null, null, 1);  
insert into sym_transform_column (transform_id, include_on, source_column_name, target_column_name, pk, transform_type, transform_expression, transform_order)
  values ('source2_to_a', 'U', 'S2', 'S2_A', 0, null, null, 2);
insert into sym_transform_column (transform_id, include_on, source_column_name, target_column_name, pk, transform_type, transform_expression, transform_order)
  values ('source2_to_a', 'D', null, 'S2_A', 0, 'const', 'DELETED', 2);

insert into sym_transform_table (transform_id, source_node_group_id, target_node_group_id, source_table_name, target_table_name, transform_point, update_first, delete_action, transform_order)
  values ('sourceb_to_b', 'test-node-group', 'test-root-group', 'SOURCE_B', 'TEST_TRANSFORM_B', 'LOAD', 1, 'DEL_ROW', 1);
insert into sym_transform_column (transform_id, include_on, source_column_name, target_column_name, pk, transform_type, transform_expression, transform_order)
  values ('sourceb_to_b', '*', 'ID', 'ID_B', 1, null, null, 1);
insert into sym_transform_column (transform_id, include_on, source_column_name, target_column_name, pk, transform_type, transform_expression, transform_order)
  values ('sourceb_to_b', '*', null, 'S1_B', 0, 'lookup', 'select 12 from sym_node_identity where  9=:ID', 2);
insert into sym_transform_column (transform_id, include_on, source_column_name, target_column_name, pk, transform_type, transform_expression, transform_order)
  values ('sourceb_to_b', '*', null, 'S2_B', 0, 'variable', 'current_timestamp', 3);
  
insert into sym_transform_table (transform_id, source_node_group_id, target_node_group_id, source_table_name, target_table_name, transform_point, update_first, delete_action, transform_order)
  values ('one_to_multi', 'test-node-group', 'test-root-group', 'SOURCE_5', 'TARGET_5', 'LOAD', 0, 'NONE', 1);
insert into sym_transform_column (transform_id, include_on, source_column_name, target_column_name, pk, transform_type, transform_expression, transform_order)
  values ('one_to_multi', '*', 'S5_ID', 'ID_TARGET', 1, 'multiply', 'select column_two from test_lookup_table where column_one=:S5_ID', 1);
insert into sym_transform_column (transform_id, include_on, source_column_name, target_column_name, pk, transform_type, transform_expression, transform_order)
  values ('one_to_multi', '*', 'S5_VALUE', 'VALUE_TARGET', 0, 'copy', null, 3);  

