--
-- Sample Data
--
insert into item_selling_price (price_id, price, cost) values (1, 0.10, 0.09);
insert into item (item_id, price_id, name) values (11000001, 1, 'Yummy Gum');

insert into transaction (tran_id, store, workstation, day, seq) 
values (900, '1', '3', '2007-11-01', 90);
insert into sale_return_line_item (tran_id, item_id, price, quantity)
values (900, 11000001, 0.10, 1);

--
-- Sample SymmetricDS Configuration
--

