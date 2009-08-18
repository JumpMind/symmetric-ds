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
values('outbound', 1, 100000, 1, 'a channel dedicated to moving data out from the home to the workstation');

insert into sym_channel 
(channel_id, processing_order, max_batch_size, enabled, description)
values('inbound', 1, 100000, 1, 'a channel dedicated to moving data out from the workstation to home');

--
-- Triggers
--

insert into sym_router  (router_id,source_node_group_id, target_node_group_id,       create_time,  last_update_time) 
                  values(   '100000',            'home',             'region', current_timestamp, current_timestamp);

insert into sym_router  (router_id,source_node_group_id, target_node_group_id,       create_time,  last_update_time) 
                  values(   '200000',          'region',        'workstation', current_timestamp, current_timestamp);  

insert into sym_trigger        (trigger_id,                source_table_name,    channel_id, sync_on_incoming_batch,  last_update_time,      create_time)
                         values(    '200000',     'sync_home_to_workstation',    'outbound',                      1, current_timestamp,current_timestamp);
insert into sym_trigger_router (trigger_id, router_id, initial_load_order,  last_update_time,       create_time)
                         values(  '100000',  '100000',                100, current_timestamp, current_timestamp);
insert into sym_trigger_router (trigger_id, router_id, initial_load_order,  last_update_time,       create_time)
                         values(  '200000',  '200000',                100, current_timestamp, current_timestamp);   
                  