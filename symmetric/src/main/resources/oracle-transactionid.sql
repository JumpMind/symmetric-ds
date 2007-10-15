CREATE OR REPLACE function fn_transaction_id
    return varchar is
    begin
       return DBMS_TRANSACTION.local_transaction_id(false);
    end;
/
CREATE OR REPLACE function fn_trigger_disabled return varchar is 
  begin 
     return pack_symmetric.disable_trigger;
  end;
/
CREATE OR REPLACE package pack_symmetric as
    disable_trigger pls_integer;
    procedure setValue (a IN number);
 end pack_symmetric;
/
CREATE OR REPLACE package body pack_symmetric as
    procedure setValue(a IN number) is
    begin
         pack_symmetric.disable_trigger:=a;
    end;
  end pack_symmetric;
/