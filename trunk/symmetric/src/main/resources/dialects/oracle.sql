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

