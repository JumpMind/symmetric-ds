insert into sym_node_group (node_group_id, description) 
values ('home', 'Home Office');

insert into sym_node_group (node_group_id, description) 
values ('region', 'Regional Waypoints');

insert into sym_node_group (node_group_id, description) 
values ('workstation', 'Client Machines');

insert into sym_node_group (node_group_id, description) 
values ('pushOnly', 'A group that only pushes data');

insert into sym_node_group_link (source_node_group_id, target_node_group_id, data_event_action)
values ('pushOnly', 'home', 'P');
insert into sym_node_group_link (source_node_group_id, target_node_group_id, data_event_action)
values ('region', 'home', 'P');
insert into sym_node_group_link (source_node_group_id, target_node_group_id, data_event_action)
values ('home', 'region', 'W');
insert into sym_node_group_link (source_node_group_id, target_node_group_id, data_event_action)
values ('workstation', 'region', 'P');
insert into sym_node_group_link (source_node_group_id, target_node_group_id, data_event_action)
values ('region', 'workstation', 'W');

insert into sym_node (node_id, node_group_id, external_id, sync_enabled)
values ('home', 'home', 'home', 1);
insert into sym_node_identity values ('home');

--
-- Channels
--
insert into sym_channel 
(channel_id, processing_order, max_batch_size, enabled, description)
values('outbound', 1, 1000, 1, 'a channel dedicated to moving data out from the home to the workstation');

insert into sym_channel 
(channel_id, processing_order, max_batch_size, enabled, description)
values('inbound', 1, 1000, 1, 'a channel dedicated to moving data out from the workstation to home');

--
-- Triggers
--
insert into sym_trigger 
(source_table_name, source_node_group_id, target_node_group_id, channel_id, sync_on_insert, sync_on_update, sync_on_delete, initial_load_order, sync_on_incoming_batch, last_update_by, last_update_time, create_time)
values('sync_home_to_workstation', 'home', 'region', 'outbound', 1, 1, 1, 100, 0, 'test', current_timestamp, current_timestamp);

insert into sym_trigger 
(source_table_name, source_node_group_id, target_node_group_id, channel_id, sync_on_insert, sync_on_update, sync_on_delete, initial_load_order, sync_on_incoming_batch, last_update_by, last_update_time, create_time)
values('sync_home_to_workstation', 'region', 'workstation', 'outbound', 1, 1, 1, 100, 1, 'test', current_timestamp, current_timestamp);

insert into sym_trigger 
(source_table_name, source_node_group_id, target_node_group_id, channel_id, sync_on_insert, sync_on_update, sync_on_delete, initial_load_order, sync_on_incoming_batch, last_update_by, last_update_time, create_time)
values('sync_workstation_to_home', 'workstation', 'region', 'inbound', 1, 1, 1, 100, 0, 'test', current_timestamp, current_timestamp);

insert into sym_trigger 
(source_table_name, source_node_group_id, target_node_group_id, channel_id, sync_on_insert, sync_on_update, sync_on_delete, initial_load_order, sync_on_incoming_batch, last_update_by, last_update_time, create_time)
values('sync_workstation_to_home', 'region', 'home', 'inbound', 1, 1, 1, 100, 1, 'test', current_timestamp, current_timestamp);
