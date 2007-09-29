drop function if exists fn_transaction_id
/
create function fn_transaction_id()
  returns varchar(50)
  begin
     declare comm_name varchar(50);
     declare comm_value varchar(50);
     declare comm_cur cursor for show status like 'Com_commit';
     if @@autocommit = 0 then
          open comm_cur;
          fetch comm_cur into comm_name, comm_value;
          close comm_cur;
          return concat(concat(connection_id(), '.'), comm_value);
     else
          return null;
     end if;
  end
/