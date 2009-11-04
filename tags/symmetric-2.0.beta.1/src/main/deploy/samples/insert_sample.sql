------------------------------------------------------------------------------
-- Sample Data
------------------------------------------------------------------------------
insert into item_selling_price (price_id, price, cost) values (1, 0.10, 0.09);
insert into item (item_id, price_id, name) values (11000001, 1, 'Yummy Gum');

insert into sale_transaction (tran_id, store, workstation, day, seq) 
values (900, '1', '3', '2007-11-01', 90);
insert into sale_return_line_item (tran_id, item_id, price, quantity, returned_quantity)
values (900, 11000001, 0.10, 1, 0);

------------------------------------------------------------------------------
-- Sample Symmetric Configuration
------------------------------------------------------------------------------
--
-- Nodes
--
insert into sym_node_group (node_group_id, description) 
values ('corp', 'Central Office');
insert into sym_node_group (node_group_id, description) 
values ('store', 'Store');

insert into sym_node_group_link (source_node_group_id, target_node_group_id, data_event_action)
values ('store', 'corp', 'P');
insert into sym_node_group_link (source_node_group_id, target_node_group_id, data_event_action)
values ('corp', 'store', 'W');

insert into sym_node (node_id, node_group_id, external_id, sync_enabled)
values ('00000', 'corp', '00000', 1);
insert into sym_node_identity values ('00000');

--
-- Channels
--
insert into sym_channel 
(channel_id, processing_order, max_batch_size, enabled, description)
values('sale_transaction', 1, 100000, 1, 'sale_transactional data from register and back office');

insert into sym_channel 
(channel_id, processing_order, max_batch_size, enabled, description)
values('item', 1, 100000, 1, 'Item and pricing data');

--
-- Triggers
--
insert into sym_trigger 
(trigger_id,source_table_name,channel_id,last_update_time,create_time)
values('item_selling_price','item_selling_price','item',current_timestamp,current_timestamp);

insert into sym_trigger 
(trigger_id,source_table_name,channel_id,last_update_time,create_time)
values('item','item','item',current_timestamp,current_timestamp);

insert into sym_trigger 
(trigger_id,source_table_name,channel_id,last_update_time,create_time)
values('sale_transaction','sale_transaction','sale_transaction',current_timestamp,current_timestamp);

-- Example of a "dead" trigger, which is used to only sync the table during initial load
insert into sym_trigger 
(trigger_id,source_table_name,channel_id, sync_on_insert, sync_on_update, sync_on_delete, last_update_time,create_time)
values('sale_transaction_dead','sale_transaction','sale_transaction',0,0,0,current_timestamp,current_timestamp);

insert into sym_trigger 
(trigger_id,source_table_name,channel_id,last_update_time,create_time)
values('sale_return_line_item','sale_return_line_item','sale_transaction',current_timestamp,current_timestamp);

--
-- Routers
--

insert into sym_router 
(router_id,source_node_group_id,target_node_group_id,create_time,last_update_time)
values('outbound_item_selling_price', 'corp', 'store', current_timestamp, current_timestamp);
insert into sym_trigger_router 
(trigger_id,router_id,initial_load_order,last_update_time,create_time)
values('item_selling_price','outbound_item_selling_price',100,current_timestamp,current_timestamp);

insert into sym_router 
(router_id,source_node_group_id,target_node_group_id,create_time,last_update_time)
values('outbound_item', 'corp', 'store', current_timestamp, current_timestamp);
insert into sym_trigger_router 
(trigger_id,router_id,initial_load_order,last_update_time,create_time)
values('item','outbound_item', 200, current_timestamp, current_timestamp);

insert into sym_router 
(router_id,source_node_group_id,target_node_group_id,create_time,last_update_time)
values('incoming_sale_transaction', 'store', 'corp', current_timestamp, current_timestamp);
insert into sym_trigger_router 
(trigger_id,router_id,initial_load_order,last_update_time,create_time)
values('sale_transaction','incoming_sale_transaction', 200, current_timestamp, current_timestamp);

-- Example of a "dead" trigger, which is used to only sync the table during initial load
insert into sym_router 
(router_id,source_node_group_id,target_node_group_id,create_time,last_update_time)
values('load_sale_transaction', 'corp', 'store', current_timestamp, current_timestamp);
insert into sym_trigger_router 
(trigger_id,router_id,initial_load_order,last_update_time,create_time)
values('sale_transaction_dead','load_sale_transaction', 300, current_timestamp, current_timestamp);

insert into sym_router 
(router_id,source_node_group_id,target_node_group_id,create_time,last_update_time)
values('inbound_sale_return_line_item', 'store', 'corp', current_timestamp, current_timestamp);
insert into sym_trigger_router 
(trigger_id,router_id,initial_load_order,last_update_time,create_time)
values('sale_return_line_item','inbound_sale_return_line_item', 205, current_timestamp, current_timestamp);

-- Example of using sync_on_incoming_batch to sync a change arriving at corp from one store back out to all stores
insert into sym_router 
(router_id,source_node_group_id,target_node_group_id,create_time,last_update_time)
values('outbound_sale_return_line_item', 'corp', 'store', current_timestamp, current_timestamp);
insert into sym_trigger_router 
(trigger_id,router_id,initial_load_order,last_update_time,create_time)
values('sale_return_line_item','outbound_sale_return_line_item', 210, current_timestamp, current_timestamp);