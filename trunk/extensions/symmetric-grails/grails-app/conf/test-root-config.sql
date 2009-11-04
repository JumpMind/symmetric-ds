insert into sym_channel (channel_id, processing_order, max_batch_size, max_batch_to_send, enabled, description) values('testchannel', 1, 50, 50, 1, null);

insert into sym_node_group values ('root','root configuration');
insert into sym_node_group values ('client','client configuration');
insert into sym_node_group_link values ('root','client', 'P');
insert into sym_node_group_link values ('client','root', 'W');




