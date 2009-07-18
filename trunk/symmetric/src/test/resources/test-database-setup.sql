insert into sym_channel (channel_id, processing_order, max_batch_size, max_batch_to_send, enabled, description) values('testchannel', 1, 50, 50, 1, null);
insert into sym_channel (channel_id, processing_order, max_batch_size, max_batch_to_send, enabled, description) values('other', 0, 1, 50, 1, null);
insert into sym_node_group values ('symmetric','a group representing symmetric configuration');
insert into sym_node_group values ('test-root-group','a test config');
insert into sym_node_group values ('test-node-group','a test config');
insert into sym_node_group values ('test-node-group2','another test config');
insert into sym_node_group values ('test-group-for-root-to-pull','another test config');
insert into sym_node_group values ('unit-test-only','a group used for unit testing');
insert into sym_node_group_link values ('test-root-group','test-root-group', 'P');
insert into sym_node_group_link values ('test-root-group','test-node-group2', 'P');
insert into sym_node_group_link values ('test-root-group','test-node-group', 'W');
insert into sym_node_group_link values ('test-group-for-root-to-pull','test-root-group', 'W');
insert into sym_node_group_link values ('test-node-group','test-root-group', 'P');
insert into sym_node_group_link values ('symmetric','test-root-group', 'P');
insert into sym_node values ('00000', 'test-root-group', '00000', 1, 'internal://root', '1', '1.4.0-SNAPSHOT','H2', '1.1', current_timestamp, null, '00000');
insert into sym_node values ('1', 'test-node-group', '1', 1, 'internal://root', '1', '1.4.0-SNAPSHOT','H2', '5.0', current_timestamp, null, '00000');
insert into sym_node values ('00001', 'test-node-group', '00001', 1, 'http://localhost:8080/sync', '1', '1.4.0-SNAPSHOT', 'H2', '5.0', current_timestamp, null, '00000');
insert into sym_node values ('00002', 'test-node-group', '00002', 0, null, null, '1.4.0-SNAPSHOT', null, null, current_timestamp, null, '00000');
insert into sym_node values ('00003', 'test-node-group', '00003', 1, 'http://localhost:8080/', '0', '1.4.0-SNAPSHOT', 'H2', '4', current_timestamp, null, '00000');
insert into sym_node values ('00010', 'test-node-group2', '00010', 1, null, null, '1.4.0-SNAPSHOT', null, null, current_timestamp, null, '00000');
insert into sym_node values ('00011', 'test-node-group2', '00011', 1, null, null, '1.4.0-SNAPSHOT', null, null, current_timestamp, null, '00000');
insert into sym_node values ('pull1', 'test-group-for-root-to-pull', 'test', 1, 'http://localhost:8080/sync', '1', '1.4.0-SNAPSHOT', 'H2', '5.0', current_timestamp, null, '00000');
insert into sym_node values ('pull2', 'test-group-for-root-to-pull', 'test', 1, null, null, '1.4.0-SNAPSHOT', null, null, current_timestamp, null, '00000');
insert into sym_node values ('pull3', 'test-group-for-root-to-pull', 'test', 1, 'http://localhost:8080/', '0', '1.4.0-SNAPSHOT', 'H2', '4', current_timestamp, null, '00000');


insert into sym_node values ('55555', 'test-node-group2', '00011', 1, 'http://snoopdog.com', null, '1.4.0-SNAPSHOT', null, null, current_timestamp, null, '00000');

-- For testFindNodesThatOriginatedHere
insert into sym_node values ('44001', 'unit-test-only', '44001', 1, null, null, null, null, null, current_timestamp, null, '00011');
insert into sym_node values ('44002', 'unit-test-only', '44001', 1, null, null, null, null, null, current_timestamp, null, '00012');
insert into sym_node values ('44003', 'unit-test-only', '44001', 1, null, null, null, null, null, current_timestamp, null, '00011');
insert into sym_node values ('44004', 'unit-test-only', '44001', 1, null, null, null, null, null, current_timestamp, null, '44002');
insert into sym_node values ('44005', 'unit-test-only', '44001', 1, null, null, null, null, null, current_timestamp, null, '44001');
insert into sym_node values ('44006', 'unit-test-only', '44001', 1, null, null, null, null, null, current_timestamp, null, '44003');

insert into sym_node_security values ('00001', 'secret', 0, {ts '2007-01-01 01:01:01'}, 0, {ts '2007-01-01 01:01:01'}, '00000');
insert into sym_node_security values ('00002', 'supersecret', 1, null, 0, null, '00000');
insert into sym_node_security values ('00003', 'notsecret', 0, {ts '2007-01-01 01:01:01'}, 0, {ts '2007-01-01 01:01:01'}, '00000');
insert into sym_node_security values ('00000', 'notsecret', 0, {ts '2007-01-01 01:01:01'}, 0, null, '00000');
insert into sym_node_identity values ('00000');

insert into sym_trigger 
(source_table_name,source_node_group_id,target_node_group_id,channel_id,sync_on_update,sync_on_insert,sync_on_delete,sync_on_update_condition,sync_on_insert_condition,sync_on_delete_condition,tx_id_expression,initial_load_order,last_updated_by,last_updated_time,name_for_insert_trigger,create_time)
values('test_triggers_table','test-root-group','test-root-group','testchannel', 1, 1, 1, null, null, null, null, 1, 'chenson', current_timestamp, 'insert_test_tbl_trg',current_timestamp);

insert into sym_trigger 
(source_table_name,source_node_group_id,target_node_group_id,channel_id,sync_on_update,sync_on_insert,sync_on_delete,sync_on_update_condition,sync_on_insert_condition,sync_on_delete_condition,tx_id_expression,initial_load_order,last_updated_by,last_updated_time,create_time)
values('sym_node_group','symmetric','test-root-group','config', 1, 1, 1, null, null, null, null, 1, 'chenson', current_timestamp, current_timestamp);

insert into sym_trigger 
(source_table_name,source_node_group_id,target_node_group_id,channel_id,sync_on_update,sync_on_insert,sync_on_delete,sync_on_incoming_batch,sync_on_update_condition,sync_on_insert_condition,sync_on_delete_condition,tx_id_expression,initial_load_order,last_updated_by,last_updated_time,name_for_insert_trigger,create_time)
values('test_sync_incoming_batch','test-root-group','test-node-group2','testchannel', 1, 1, 1, 1, null, null, null, null, 1, 'erilong', current_timestamp,null,current_timestamp);

-- AdditiveDataLoaderFilter test data
insert into TEST_ADD_DL_TABLE_1 values('k3','k4',1,2.0,3,4.0,5,'6',7);
insert into TEST_ADD_DL_TABLE_1 values('k5','k6',1,3.0,5,7.0,9,'11',13);
insert into TEST_ADD_DL_TABLE_2 values('k3',1);





