insert into sym_channel (channel_id, processing_order, max_batch_size, max_batch_to_send, contains_big_lob, enabled, description) values('testchannel', 1, 50, 50, 1, 1, null);

insert into sym_node_group values ('test-root-group','a test config');
insert into sym_node_group values ('test-node-group','a test config');
insert into sym_node_group_link values ('test-node-group','test-root-group', 'P');
insert into sym_node_group_link values ('test-root-group','test-node-group', 'W');

insert into sym_node values ('00000', 'test-root-group', '00000', 1, null, null, '2.0', null, null, current_timestamp, null, 0, 0, '00000', 'engine');
insert into sym_node_identity values ('00000');

  
insert into sym_router  (router_id,source_node_group_id, target_node_group_id,       create_time,  last_update_time) 
                  values(   '1000',   'test-root-group',    'test-node-group', current_timestamp, current_timestamp);

insert into sym_router  (router_id,source_node_group_id, target_node_group_id,       create_time,  last_update_time) 
                  values(   '2000',   'test-node-group',    'test-root-group', current_timestamp, current_timestamp);  

insert into sym_trigger        (trigger_id,                source_table_name,    channel_id,                          tx_id_expression,  last_update_time,      create_time)
                         values(    '1000', 'test_very_long_table_name_1234', 'testchannel', '$(curTriggerValue).$(curColumnPrefix)id', current_timestamp,current_timestamp);
insert into sym_trigger_router (trigger_id, router_id, initial_load_order,  last_update_time,       create_time)
                         values(    '1000',    '1000',                  1, current_timestamp, current_timestamp);
  
insert into sym_trigger        (trigger_id,                source_table_name,    channel_id,  last_update_time,      create_time)
                         values(    '2000',            'test_triggers_table', 'testchannel', current_timestamp,current_timestamp);
insert into sym_trigger_router (trigger_id, router_id, initial_load_order,  last_update_time,       create_time)
                         values(    '2000',    '1000',                  1, current_timestamp, current_timestamp);
insert into sym_trigger_router (trigger_id, router_id, initial_load_order,  last_update_time,       create_time)
                         values(    '2000',    '2000',                  1, current_timestamp, current_timestamp);   

insert into sym_trigger        (trigger_id,                source_table_name,    channel_id,  last_update_time,      create_time)
                         values(    '3000',                  'test_customer', 'testchannel', current_timestamp,current_timestamp);
insert into sym_trigger_router (trigger_id, router_id, initial_load_order,  last_update_time,       create_time)
                         values(    '3000',    '1000',                  1, current_timestamp, current_timestamp);

insert into sym_trigger        (trigger_id,                source_table_name,    channel_id,  last_update_time,      create_time)
                         values(    '4000',              'test_order_header', 'testchannel', current_timestamp,current_timestamp);
insert into sym_trigger_router (trigger_id, router_id, initial_load_order,  last_update_time,       create_time)
                         values(    '4000',    '2000',                  1, current_timestamp, current_timestamp);

insert into sym_trigger        (trigger_id,                source_table_name,    channel_id,                              sync_on_update_condition,                              sync_on_insert_condition,  last_update_time,      create_time)
                         values(    '4500',              'test_order_header', 'testchannel', '$(newTriggerValue).$(newColumnPrefix)status = ''C''', '$(newTriggerValue).$(newColumnPrefix)status = ''C''', current_timestamp,current_timestamp);
insert into sym_router  (router_id,source_node_group_id, target_node_group_id,   router_type, router_expression,       create_time,  last_update_time) 
                  values(   '4500',   'test-root-group',    'test-node-group',      'column',         'STATUS=C', current_timestamp, current_timestamp);  
insert into sym_trigger_router (trigger_id, router_id, initial_load_order,  last_update_time,       create_time)
                         values(    '4500',    '4500',                  1, current_timestamp, current_timestamp);

insert into sym_trigger        (trigger_id,                source_table_name,    channel_id,  last_update_time,      create_time)
                         values(    '5000',              'test_order_detail', 'testchannel', current_timestamp,current_timestamp);
insert into sym_trigger_router (trigger_id, router_id, initial_load_order,  last_update_time,       create_time)
                         values(    '5000',    '1000',                  1, current_timestamp, current_timestamp);

insert into sym_trigger        (trigger_id,                source_table_name,    channel_id,  last_update_time,      create_time)
                         values(    '6000',              'test_store_status', 'testchannel', current_timestamp,current_timestamp);
insert into sym_trigger_router (trigger_id, router_id, initial_load_order,  last_update_time,       create_time)
                         values(    '6000',    '2000',                  1, current_timestamp, current_timestamp);

insert into sym_trigger        (trigger_id,                source_table_name,    channel_id, sync_on_update,sync_on_insert,sync_on_delete, last_update_time,      create_time)
                         values(    '7000',                  'test_key_word', 'testchannel',              0,             0,             0, current_timestamp,current_timestamp);
insert into sym_trigger_router (trigger_id, router_id, initial_load_order,  last_update_time,       create_time)
                         values(    '7000',    '1000',                  1, current_timestamp, current_timestamp);

insert into sym_trigger        (trigger_id,                source_table_name,    channel_id,  last_update_time,      create_time)
                         values(    '8000',                  'TEST_ALL_CAPS', 'testchannel', current_timestamp,current_timestamp);
insert into sym_trigger_router (trigger_id, router_id, initial_load_order,  last_update_time,       create_time)
                         values(    '8000',    '1000',                  1, current_timestamp, current_timestamp);

insert into sym_trigger        (trigger_id,                source_table_name,    channel_id,  last_update_time,      create_time)
                         values(    '9000',                'Test_Mixed_Case', 'testchannel', current_timestamp,current_timestamp);
insert into sym_trigger_router (trigger_id, router_id, initial_load_order,  last_update_time,       create_time)
                         values(    '9000',    '1000',                  1, current_timestamp, current_timestamp);

insert into sym_trigger        (trigger_id,                source_table_name,    channel_id,  last_update_time,      create_time)
                         values(  '10000',               'one_column_table', 'testchannel', current_timestamp,current_timestamp);
insert into sym_trigger_router (trigger_id, router_id, initial_load_order,  last_update_time,       create_time)
                         values(  '10000',    '1000',                  1, current_timestamp, current_timestamp);

insert into sym_trigger        (trigger_id,                source_table_name,    channel_id,  last_update_time,      create_time)
                         values(  '11000',           'no_primary_key_table', 'testchannel', current_timestamp,current_timestamp);
insert into sym_trigger_router (trigger_id, router_id, initial_load_order,  last_update_time,       create_time)
                         values(  '11000',    '1000',                  1, current_timestamp, current_timestamp);

insert into sym_trigger        (trigger_id,                source_table_name,    channel_id, last_update_time,      create_time)
                         values(  '12000',         'test_sync_column_level', 'testchannel',  current_timestamp,current_timestamp);
insert into sym_trigger_router (trigger_id, router_id, initial_load_order,  last_update_time,       create_time)
                         values(  '12000',    '1000',                  1, current_timestamp, current_timestamp);

insert into sym_trigger        (trigger_id,                source_table_name,    channel_id, last_update_time,      create_time)
                         values(  '13000',            'test_target_table_a', 'testchannel',  current_timestamp,current_timestamp);
insert into sym_router  (router_id,source_node_group_id, target_node_group_id,     target_table_name,       create_time,  last_update_time) 
                  values( '13000',   'test-root-group',    'test-node-group', 'test_target_table_b', current_timestamp, current_timestamp);  
insert into sym_trigger_router (trigger_id, router_id, initial_load_order,  last_update_time,       create_time)
                         values(  '13000',  '13000',                  1, current_timestamp, current_timestamp);


insert into test_customer
(customer_id, name, is_active, address, city, state, zip, entry_timestamp, entry_time)
values(100, 'John Smith', '1', '300 Main Street', 'Columbus', 'OH', 43230, {ts '2007-01-02 11:30:00'}, null);

insert into test_order_header
(order_id, customer_id, status, deliver_date)
values('1', 100, null, {d '2007-01-02'});

insert into test_order_detail
(order_id, line_number, item_type, item_id, quantity, price)
values('1', 1, 'STCK', '110000055', 5, 1.29);

insert into test_sync_column_level
(id, string_value, time_value, date_value, bigint_value, decimal_value)
values (1, 'data', {ts '2008-01-02 03:04:05'}, {d '2008-01-02'}, 100, 123.45);
