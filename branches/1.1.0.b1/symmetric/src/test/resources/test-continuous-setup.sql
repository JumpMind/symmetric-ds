insert into sym_channel values('testchannel', 1, 50, 1, null);
insert into sym_channel values('config', 1, 50, 1, null);
insert into sym_node_group values ('symmetric','a group representing symmetric configuration');
insert into sym_node_group values ('test-root-group','a test config');
insert into sym_node_group values ('test-node-group','a test config');
insert into sym_node_group values ('CORP','Central Office');
insert into sym_node_group values ('STORE','Store');
insert into sym_node_group_link values ('test-root-group','test-root-group', 'P');
insert into sym_node_group_link values ('test-root-group','test-node-group', 'W');
insert into sym_node_group_link values ('symmetric','test-root-group', 'P');
insert into sym_node_group_link values ('STORE','CORP', 'P');
insert into sym_node_group_link values ('CORP','STORE', 'W');
insert into sym_node values ('00000', 'CORP', '00000', '1', 'http://centraloffice:8080/sync', 1, '1.1','Oracle', '9', current_timestamp);
insert into sym_node values ('00001', 'STORE', '00001', '1', 'http://localhost:8080/sync', 1, '1.1', 'MySQL', '5', current_timestamp);
insert into sym_node values ('00002', 'STORE', '00002', '0', null, null, '1.1', null, null, current_timestamp);
insert into sym_node values ('00003', 'STORE', '00003', '1', 'http://localhost:8080/', 0, '1.1', 'MySql', '4', current_timestamp);
insert into sym_node_security values ('00001', 'secret', '0', {ts '2007-01-01 01:01:01'});
insert into sym_node_security values ('00002', 'supersecret', '1', null);
insert into sym_node_security values ('00003', 'notsecret', '0', {ts '2007-01-01 01:01:01'});
insert into sym_node_identity values ('00001');

insert into sym_trigger 
(source_table_name,source_node_group_id,target_node_group_id,channel_id,sync_on_update,sync_on_insert,sync_on_delete,sync_on_update_condition,sync_on_insert_condition,sync_on_delete_condition,initial_load_select,node_select,tx_id_expression,initial_load_order,last_updated_by,last_updated_time,name_for_insert_trigger,create_time)
values('test_triggers_table','test-root-group','test-root-group','testchannel', 1, 1, 1, null, null, null, null, null, null, 1, 'chenson', current_timestamp, 'insert_test_tbl_trg',current_timestamp);

insert into sym_trigger 
(source_table_name,source_node_group_id,target_node_group_id,channel_id,sync_on_update,sync_on_insert,sync_on_delete,sync_on_update_condition,sync_on_insert_condition,sync_on_delete_condition,initial_load_select,node_select,tx_id_expression,initial_load_order,last_updated_by,last_updated_time,create_time)
values('sym_node_group','symmetric','test-root-group','config', 1, 1, 1, null, null, null, null, null, null, 1, 'chenson', current_timestamp, current_timestamp);

insert into test_dataloader_table
(id, string_value, string_required_value, char_value, char_required_value,
    date_value, time_value, boolean_value, integer_value, decimal_value)
values (1, 'string', 'string not null', 'char', 'char not null',
    {d '2007-02-03'}, {ts '2007-01-02 03:04:05.06'}, '1', 42, 99.99);
insert into test_dataloader_table
(id, string_value, string_required_value, char_value, char_required_value,
    date_value, time_value, boolean_value, integer_value, decimal_value)
values (5, 'string', 'string not null', 'char', 'char not null',
    {d '2007-02-03'}, {ts '2007-01-02 03:04:05.06'}, '1', 42, 99.99);