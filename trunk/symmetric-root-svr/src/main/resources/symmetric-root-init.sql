insert into sym_node_group values ('ROOT','');
insert into sym_node_group values ('NODE','');
insert into sym_node_group_link values ('NODE','ROOT', 'P');
insert into sym_node_group_link values ('ROOT','NODE', 'W');
insert into sym_node values ('00000', 'ROOT', '00000', 1, null, null, null, null, null, current_timestamp, null);
insert into sym_node_identity values ('00000');