CREATE OR REPLACE package pack_symmetric as
    disable_trigger pls_integer;
    disable_node_id varchar(50);
    procedure setValue (a IN number);
    procedure setNodeValue (node_id IN varchar);
 end pack_symmetric;
/
CREATE OR REPLACE package body pack_symmetric as
    procedure setValue(a IN number) is
    begin
         pack_symmetric.disable_trigger:=a;
    end;
    procedure setNodeValue(node_id IN varchar) is
    begin
         pack_symmetric.disable_node_id := node_id;
    end;
end pack_symmetric;
/

