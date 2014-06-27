insert into sym_channel values('testchannel', 1, 50, 50, 1, null);

insert into sym_node_group values ('test-root-group','a test config');
insert into sym_node_group values ('test-node-group','a test config');
insert into sym_node_group_link values ('test-node-group','test-root-group', 'P');
insert into sym_node_group_link values ('test-root-group','test-node-group', 'W');

insert into sym_node values ('00000', 'test-root-group', '00000', 1, null, null, '1.4.0-SNAPSHOT', null, null, current_timestamp, null);
insert into sym_node_identity values ('00000');

insert into sym_trigger 
(source_table_name,source_node_group_id,target_node_group_id,channel_id,sync_on_update,sync_on_insert,sync_on_delete,sync_on_update_condition,sync_on_insert_condition,sync_on_delete_condition,initial_load_select,node_select,tx_id_expression,initial_load_order,last_updated_by,last_updated_time,name_for_insert_trigger,create_time)
values('test_very_long_table_name_1234','test-root-group','test-node-group','testchannel', 1, 1, 1, null, null, null, null, null, '$(curTriggerValue).id', 1, 'chenson', current_timestamp,null,current_timestamp);

insert into sym_trigger 
(source_table_name,source_node_group_id,target_node_group_id,channel_id,sync_on_update,sync_on_insert,sync_on_delete,sync_on_update_condition,sync_on_insert_condition,sync_on_delete_condition,initial_load_select,node_select,tx_id_expression,initial_load_order,last_updated_by,last_updated_time,name_for_insert_trigger,create_time)
values('test_triggers_table','test-root-group','test-node-group','testchannel', 1, 1, 1, null, null, null, null, null, null, 1, 'chenson', current_timestamp,null,current_timestamp);

insert into sym_trigger 
(source_table_name,source_node_group_id,target_node_group_id,channel_id,sync_on_update,sync_on_insert,sync_on_delete,sync_on_update_condition,sync_on_insert_condition,sync_on_delete_condition,initial_load_select,node_select,tx_id_expression,initial_load_order,last_updated_by,last_updated_time,name_for_insert_trigger,create_time)
values('test_triggers_table','test-node-group','test-root-group','testchannel', 1, 1, 1, null, null, null, null, null, null, 1, 'chenson', current_timestamp,null,current_timestamp);

insert into sym_trigger 
(source_table_name,source_node_group_id,target_node_group_id,channel_id,sync_on_update,sync_on_insert,sync_on_delete,sync_on_update_condition,sync_on_insert_condition,sync_on_delete_condition,initial_load_select,node_select,tx_id_expression,initial_load_order,last_updated_by,last_updated_time,name_for_insert_trigger,create_time)
values('test_customer','test-root-group','test-node-group','testchannel', 1, 1, 1, null, null, null, null, null, null, 1, 'erilong', current_timestamp,null,current_timestamp);

insert into sym_trigger 
(source_table_name,source_node_group_id,target_node_group_id,channel_id,sync_on_update,sync_on_insert,sync_on_delete,sync_on_update_condition,sync_on_insert_condition,sync_on_delete_condition,initial_load_select,node_select,tx_id_expression,initial_load_order,last_updated_by,last_updated_time,name_for_insert_trigger,create_time)
values('test_order_header','test-node-group','test-root-group','testchannel', 1, 1, 1, null, null, null, null, null, null, 1, 'erilong', current_timestamp,null,current_timestamp);

insert into sym_trigger 
(source_table_name,source_node_group_id,target_node_group_id,channel_id,sync_on_update,sync_on_insert,sync_on_delete,sync_on_update_condition,sync_on_insert_condition,sync_on_delete_condition,initial_load_select,node_select,tx_id_expression,initial_load_order,last_updated_by,last_updated_time,name_for_insert_trigger,create_time)
values('test_order_header','test-root-group','test-node-group','testchannel', 1, 1, 1,'$(newTriggerValue).status = ''C''', '$(newTriggerValue).status = ''C''', null, 't.status = ''C''', null, null, 1, 'erilong', current_timestamp,null,current_timestamp);

insert into sym_trigger 
(source_table_name,source_node_group_id,target_node_group_id,channel_id,sync_on_update,sync_on_insert,sync_on_delete,sync_on_update_condition,sync_on_insert_condition,sync_on_delete_condition,initial_load_select,node_select,tx_id_expression,initial_load_order,last_updated_by,last_updated_time,name_for_insert_trigger,create_time)
values('test_order_detail','test-node-group','test-root-group','testchannel', 1, 1, 1, null, null, null, null, null, null, 1, 'erilong', current_timestamp,null,current_timestamp);

insert into sym_trigger 
(source_table_name,source_node_group_id,target_node_group_id,channel_id,sync_on_update,sync_on_insert,sync_on_delete,sync_on_update_condition,sync_on_insert_condition,sync_on_delete_condition,initial_load_select,node_select,tx_id_expression,initial_load_order,last_updated_by,last_updated_time,name_for_insert_trigger,create_time)
values('test_store_status','test-node-group','test-root-group','testchannel', 1, 1, 1, null, null, null, null, null, null, 1, 'erilong', current_timestamp,null,current_timestamp);

insert into sym_trigger 
(source_table_name,source_node_group_id,target_node_group_id,channel_id,sync_on_update,sync_on_insert,sync_on_delete,sync_on_update_condition,sync_on_insert_condition,sync_on_delete_condition,initial_load_select,node_select,tx_id_expression,initial_load_order,last_updated_by,last_updated_time,name_for_insert_trigger,create_time)
values('TEST_ALL_CAPS','test-root-group','test-node-group','testchannel', 1, 1, 1, null, null, null, null, null, null, 1, 'erilong', current_timestamp,null,current_timestamp);

insert into sym_trigger 
(source_table_name,source_node_group_id,target_node_group_id,channel_id,sync_on_update,sync_on_insert,sync_on_delete,sync_on_update_condition,sync_on_insert_condition,sync_on_delete_condition,initial_load_select,node_select,tx_id_expression,initial_load_order,last_updated_by,last_updated_time,name_for_insert_trigger,create_time)
values('Test_Mixed_Case','test-root-group','test-node-group','testchannel', 1, 1, 1, null, null, null, null, null, null, 1, 'erilong', current_timestamp,null,current_timestamp);

insert into sym_trigger 
(source_table_name,source_node_group_id,target_node_group_id,channel_id,sync_on_update,sync_on_insert,sync_on_delete,sync_on_update_condition,sync_on_insert_condition,sync_on_delete_condition,initial_load_select,node_select,tx_id_expression,initial_load_order,last_updated_by,last_updated_time,name_for_insert_trigger,create_time)
values('ONE_COLUMN_TABLE','test-root-group','test-node-group','testchannel', 1, 1, 1, null, null, null, null, null, null, 1, 'erilong', current_timestamp,null,current_timestamp);

insert into sym_trigger 
(source_table_name,source_node_group_id,target_node_group_id,channel_id,sync_on_update,sync_on_insert,sync_on_delete,sync_on_update_condition,sync_on_insert_condition,sync_on_delete_condition,initial_load_select,node_select,tx_id_expression,initial_load_order,last_updated_by,last_updated_time,name_for_insert_trigger,create_time)
values('NO_PRIMARY_KEY_TABLE','test-root-group','test-node-group','testchannel', 1, 1, 1, null, null, null, null, null, null, 1, 'erilong', current_timestamp,null,current_timestamp);

insert into test_customer
(customer_id, name, is_active, address, city, state, zip, entry_time)
values(100, 'John Smith', '1', '300 Main Street', 'Columbus', 'OH', 43230, {ts '2007-01-02 11:30:00'});

insert into test_order_header
(order_id, customer_id, status, deliver_date)
values('1', 100, null, {d '2007-01-02'});

insert into test_order_detail
(order_id, line_number, item_type, item_id, quantity, price)
values('1', 1, 'STCK', '110000055', 5, 1.29);
