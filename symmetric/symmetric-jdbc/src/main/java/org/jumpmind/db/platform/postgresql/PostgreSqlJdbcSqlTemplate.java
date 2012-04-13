package org.jumpmind.db.platform.postgresql;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabasePlatformSettings;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.springframework.jdbc.support.lob.LobHandler;

public class PostgreSqlJdbcSqlTemplate extends JdbcSqlTemplate {

    public PostgreSqlJdbcSqlTemplate(DataSource dataSource, DatabasePlatformSettings settings,
            LobHandler lobHandler) {
        super(dataSource, settings, lobHandler);
        this.requiresAutoCommitFalseToSetFetchSize = true;
        primaryKeyViolationSqlStates = new String[] { "23505" };
    }

    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        if (PostgreSqlDdlBuilder.isUsePseudoSequence()) {
            return "select seq_id from " + sequenceName + "_tbl";
        } else {
            return "select currval('" + sequenceName + "_seq')";
        }
    }

    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }

}
