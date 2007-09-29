 create or replace function fn_transaction_id
    return varchar is
    begin
       return DBMS_TRANSACTION.local_transaction_id(false);
    end;
/
 CREATE OR REPLACE function isTriggerDisabled   return varchar is 
  begin 
     return pack_var.disable_trigger;
  end;
/
CREATE OR REPLACE package pack_var as
    disable_trigger pls_integer;
    procedure setValue (a IN number);
 end pack_var;
/
 CREATE OR REPLACE package body pack_var as
    procedure setValue(a IN number) is
    begin
         pack_var.disable_trigger:=a;
    end;
  end pack_var;
/