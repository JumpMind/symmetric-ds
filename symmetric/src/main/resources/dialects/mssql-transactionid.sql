IF (OBJECT_ID('fn_transaction_id')) is not null DROP PROCEDURE fn_transaction_id
/
CREATE PROCEDURE fn_transaction_id AS
  DECLARE @bind_token varchar(255);
  IF (@@TRANCOUNT > 0) BEGIN
    EXECUTE sp_getbindtoken @bind_token OUTPUT;
  END
  SELECT @bind_token AS transaction_id
  RETURN 0
/