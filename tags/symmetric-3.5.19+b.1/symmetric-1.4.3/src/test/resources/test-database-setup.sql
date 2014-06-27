insert into sym_channel values('testchannel', 1, 50, 50, 1, null);
insert into sym_channel values('config', 0, 1, 50, 1, null);
insert into sym_channel values('other', 0, 1, 50, 1, null);
insert into sym_node_group values ('symmetric','a group representing symmetric configuration');
insert into sym_node_group values ('test-root-group','a test config');
insert into sym_node_group values ('test-node-group','a test config');
insert into sym_node_group_link values ('test-root-group','test-root-group', 'P');
insert into sym_node_group_link values ('test-node-group','test-root-group', 'W');
insert into sym_node_group_link values ('symmetric','test-root-group', 'P');
insert into sym_node values ('00000', 'test-root-group', '00000', 1, 'internal://root', '1', '1.4.0-SNAPSHOT','MySQL', '5.0', current_timestamp, null);
insert into sym_node values ('1', 'test-node-group', '1', 1, 'internal://root', '1', '1.4.0-SNAPSHOT','MySQL', '5.0', current_timestamp, null);
insert into sym_node values ('00001', 'test-node-group', '00001', 1, 'http://localhost:8080/sync', '1', '1.4.0-SNAPSHOT', 'MySQL', '5.0', current_timestamp, null);
insert into sym_node values ('00002', 'test-node-group', '00002', 0, null, null, '1.4.0-SNAPSHOT', null, null, current_timestamp, null);
insert into sym_node values ('00003', 'test-node-group', '00003', 1, 'http://localhost:8080/', '0', '1.4.0-SNAPSHOT', 'MySql', '4', current_timestamp, null);
insert into sym_node_security values ('00001', 'secret', 0, {ts '2007-01-01 01:01:01'}, 0, {ts '2007-01-01 01:01:01'});
insert into sym_node_security values ('00002', 'supersecret', 1, null, 0, null);
insert into sym_node_security values ('00003', 'notsecret', 0, {ts '2007-01-01 01:01:01'}, 0, {ts '2007-01-01 01:01:01'});
insert into sym_node_identity values ('00000');

insert into sym_trigger 
(source_table_name,source_node_group_id,target_node_group_id,channel_id,sync_on_update,sync_on_insert,sync_on_delete,sync_on_update_condition,sync_on_insert_condition,sync_on_delete_condition,initial_load_select,node_select,tx_id_expression,initial_load_order,last_updated_by,last_updated_time,name_for_insert_trigger,create_time)
values('test_triggers_table','test-root-group','test-root-group','testchannel', 1, 1, 1, null, null, null, null, null, null, 1, 'chenson', current_timestamp, 'insert_test_tbl_trg',current_timestamp);

insert into sym_trigger 
(source_table_name,source_node_group_id,target_node_group_id,channel_id,sync_on_update,sync_on_insert,sync_on_delete,sync_on_update_condition,sync_on_insert_condition,sync_on_delete_condition,initial_load_select,node_select,tx_id_expression,initial_load_order,last_updated_by,last_updated_time,create_time)
values('sym_node_group','symmetric','test-root-group','config', 1, 1, 1, null, null, null, null, null, null, 1, 'chenson', current_timestamp, current_timestamp);
