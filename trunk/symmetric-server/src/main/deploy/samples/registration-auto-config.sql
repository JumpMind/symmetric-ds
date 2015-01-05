
delete from sym_node_group;
insert into sym_node_group (node_group_id,description) 
 values ('corp','DEMO Central Office group that represents the registration server and server node');
insert into sym_node_group (node_group_id,description) 
 values ('store','DEMO Store group that represents client nodes');

delete from sym_node_group_link;
insert into sym_node_group_link (source_node_group_id,target_node_group_id,data_event_action) 
 values ('store','corp','P');
insert into sym_node_group_link (source_node_group_id,target_node_group_id,data_event_action) 
 values ('corp','store','W');

delete from sym_node;
insert into sym_node (node_id,node_group_id,external_id,sync_enabled,sync_url,schema_version,symmetric_version,database_type,database_version,heartbeat_time,timezone_offset,batch_to_send_count,batch_in_error_count,created_at_node_id) 
 values ('000','corp','000',1,null,null,null,null,null,current_timestamp,null,0,0,'000');
insert into sym_node (node_id,node_group_id,external_id,sync_enabled,sync_url,schema_version,symmetric_version,database_type,database_version,heartbeat_time,timezone_offset,batch_to_send_count,batch_in_error_count,created_at_node_id) 
 values ('001','store','001',1,null,null,null,null,null,current_timestamp,null,0,0,'000');
insert into sym_node (node_id,node_group_id,external_id,sync_enabled,sync_url,schema_version,symmetric_version,database_type,database_version,heartbeat_time,timezone_offset,batch_to_send_count,batch_in_error_count,created_at_node_id) 
 values ('002','store','002',1,null,null,null,null,null,current_timestamp,null,0,0,'000');

 
delete from sym_node_security;
insert into sym_node_security (node_id,node_password,registration_enabled,registration_time,initial_load_enabled,initial_load_time,created_at_node_id) 
 values ('000','5d1c92bbacbe2edb9e1ca5dbb0e481',0,current_timestamp,0,current_timestamp,'000');
insert into sym_node_security (node_id,node_password,registration_enabled,registration_time,initial_load_enabled,initial_load_time,created_at_node_id) 
 values ('001','5d1c92bbacbe2edb9e1ca5dbb0e481',1,null,1,null,'000');
insert into sym_node_security (node_id,node_password,registration_enabled,registration_time,initial_load_enabled,initial_load_time,created_at_node_id) 
 values ('002','5d1c92bbacbe2edb9e1ca5dbb0e481',1,null,1,null,'000');

delete from sym_node_identity;
insert into sym_node_identity values ('000');
