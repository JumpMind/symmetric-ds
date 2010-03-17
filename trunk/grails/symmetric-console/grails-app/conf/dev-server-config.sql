insert into sym_channel (channel_id, processing_order, max_batch_size, max_batch_to_send, enabled, description) values('testchannel', 1, 50, 50, 1, null);

insert into sym_node_group values ('root','root configuration');
insert into sym_node_group values ('region','region configuration');
insert into sym_node_group values ('workstation','workstation configuration');

insert into sym_node_group_link values ('root','region', 'W');
insert into sym_node_group_link values ('region','root', 'P');

insert into sym_node_group_link values ('region','workstation', 'W');
insert into sym_node_group_link values ('workstation','region', 'P');


create table test_table_1 (test_id varchar(50) not null, test_value varchar(50));
create table test_table_2 (test_id varchar(50) not null, test_value varchar(50));

insert into sym_trigger (trigger_id, source_table_name, channel_id, last_update_time, create_time)
values('trigger_with_no_table','trigger_with_no_table', 'testchannel', current_timestamp, current_timestamp);


insert into sym_node (node_id, node_group_id, external_id, sync_enabled, sync_url, schema_version, symmetric_version, database_type, database_version, heartbeat_time, timezone_offset, batch_to_send_count, batch_in_error_count, created_at_node_id)
  values ('region01', 'region', 'region01', 1, 'http://region01.com', '1', '2.0','Oracle', '10g', current_timestamp, null, 0,0, 'root');
insert into sym_node (node_id, node_group_id, external_id, sync_enabled, sync_url, schema_version, symmetric_version, database_type, database_version, heartbeat_time, timezone_offset, batch_to_send_count, batch_in_error_count, created_at_node_id)
  values ('region02', 'region', 'region02', 1, 'http://region02.com', '1', '2.0','Oracle', '10g', current_timestamp, null, 0,0, 'root');
insert into sym_node (node_id, node_group_id, external_id, sync_enabled, sync_url, schema_version, symmetric_version, database_type, database_version, heartbeat_time, timezone_offset, batch_to_send_count, batch_in_error_count, created_at_node_id)
  values ('workstation01-01', 'workstation', 'workstation01-01', 1, 'http://workstation01-01', '1', '2.0','MySQL', '5.3', current_timestamp, null, 0,0, 'region01');
insert into sym_node (node_id, node_group_id, external_id, sync_enabled, sync_url, schema_version, symmetric_version, database_type, database_version, heartbeat_time, timezone_offset, batch_to_send_count, batch_in_error_count, created_at_node_id)
  values ('workstation02-01', 'workstation', 'workstation02-01', 1, 'http://workstation02-01', '1', '2.0','MySQL', '5.3', current_timestamp, null, 0,0, 'region01');
insert into sym_node (node_id, node_group_id, external_id, sync_enabled, sync_url, schema_version, symmetric_version, database_type, database_version, heartbeat_time, timezone_offset, batch_to_send_count, batch_in_error_count, created_at_node_id)
  values ('workstation01-02', 'workstation', 'workstation01-02', 1, 'http://workstation01-02', '1', '2.0','MySQL', '5.3', current_timestamp, null, 0,0, 'region02');


