package org.jumpmind.db.platform.ingres;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.sql.SymmetricLobHandler;

public class IngresJdbcSqlTemplate extends JdbcSqlTemplate {

    public IngresJdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings, SymmetricLobHandler lobHandler,
            DatabaseInfo databaseInfo) {
        super(dataSource, settings, lobHandler, databaseInfo);
        this.requiresAutoCommitFalseToSetFetchSize = true;
        primaryKeyViolationSqlStates = new String[] { "23501"};
        primaryKeyViolationMessageParts = new String[] {"Duplicate key on INSERT detected"};
        uniqueKeyViolationNameRegex = new String[] { "violates unique constraint \"(.*)\"" };
        foreignKeyViolationMessageParts = new String[] {"violates foreign key constraint"};
        foreignKeyViolationSqlStates = new String[] { "23503"};
        foreignKeyChildExistsViolationSqlStates = new String[] {"23504"};
        foreignKeyChildExistsViolationMessageParts = new String[] { "is still referenced from table" };
    }
    
    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        if (IngresDdlBuilder.isUsePseudoSequence()) {
            return "select seq_id from " + sequenceName + "_tbl";
        } else {
            return "select getGeneratedKeys()";
        }
    }
    
    @Override
    public boolean isDataTruncationViolation(Throwable ex) {
            boolean dataTruncationViolation = false;
            SQLException sqlEx = findSQLException(ex);
            if (sqlEx != null) {
                String sqlState = sqlEx.getSQLState();
                if (sqlState != null && sqlState.equals("22001")) {
                    dataTruncationViolation = true;
                }
            }
        return dataTruncationViolation;
    }
    
    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }

}
