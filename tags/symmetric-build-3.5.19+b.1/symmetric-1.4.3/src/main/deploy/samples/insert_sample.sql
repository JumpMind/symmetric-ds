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

insert into sym_node (node_id, node_group_id, external_id, sync_enabled, symmetric_version)
values ('00000', 'corp', '00000', 1, '1.4.0');
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
(source_table_name, source_node_group_id, target_node_group_id, channel_id, sync_on_insert, sync_on_update, sync_on_delete, initial_load_order, last_updated_by, last_updated_time, create_time)
values('item_selling_price', 'corp', 'store', 'item', 1, 1, 1, 100, 'demo', current_timestamp, current_timestamp);

insert into sym_trigger 
(source_table_name, source_node_group_id, target_node_group_id, channel_id, sync_on_insert, sync_on_update, sync_on_delete, initial_load_order, last_updated_by, last_updated_time, create_time)
values('item', 'corp', 'store', 'item', 1, 1, 1, 105, 'demo', current_timestamp, current_timestamp);

insert into sym_trigger 
(source_table_name, source_node_group_id, target_node_group_id, channel_id, sync_on_insert, sync_on_update, sync_on_delete, initial_load_order, last_updated_by, last_updated_time, create_time)
values('sale_transaction', 'store', 'corp', 'sale_transaction', 1, 1, 1, 200, 'demo', current_timestamp, current_timestamp);

-- Example of a "dead" trigger, which is used to only sync the table during initial load
insert into sym_trigger 
(source_table_name, source_node_group_id, target_node_group_id, channel_id, sync_on_insert, sync_on_update, sync_on_delete, initial_load_order, last_updated_by, last_updated_time, create_time)
values('sale_transaction', 'corp', 'store', 'sale_transaction', 0, 0, 0, 200, 'demo', current_timestamp, current_timestamp);

insert into sym_trigger 
(source_table_name, source_node_group_id, target_node_group_id, channel_id, sync_on_insert, sync_on_update, sync_on_delete, initial_load_order, last_updated_by, last_updated_time, create_time)
values('sale_return_line_item', 'store', 'corp', 'sale_transaction', 1, 1, 1, 205, 'demo', current_timestamp, current_timestamp);

-- Example of using sync_on_incoming_batch to sync a change arriving at corp from one store back out to all stores
insert into sym_trigger 
(source_table_name, source_node_group_id, target_node_group_id, channel_id, sync_on_insert, sync_on_update, sync_on_delete, sync_on_incoming_batch, initial_load_order, last_updated_by, last_updated_time, create_time)
values('sale_return_line_item', 'corp', 'store', 'sale_transaction', 1, 1, 1, 1, 205, 'demo', current_timestamp, current_timestamp);
