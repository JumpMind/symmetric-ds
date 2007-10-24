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
CREATE OR REPLACE FUNCTION fn_sym_blob2clob (blob_in IN BLOB)
RETURN CLOB
AS
	v_clob    CLOB := null;
	v_varchar VARCHAR2(32767);
	v_start	  PLS_INTEGER := 1;
	v_buffer  PLS_INTEGER := 32767;
BEGIN
	IF blob_in IS NOT NULL THEN
		DBMS_LOB.CREATETEMPORARY(v_clob, TRUE);
		FOR i IN 1..CEIL(DBMS_LOB.GETLENGTH(blob_in) / v_buffer)
		LOOP	
			v_varchar := UTL_RAW.CAST_TO_VARCHAR2(DBMS_LOB.SUBSTR(blob_in, v_buffer, v_start));
			DBMS_LOB.WRITEAPPEND(v_clob, LENGTH(v_varchar), v_varchar);
			v_start := v_start + v_buffer;
		END LOOP;
	END IF;
	RETURN v_clob; 
END fn_sym_blob2clob;
/

