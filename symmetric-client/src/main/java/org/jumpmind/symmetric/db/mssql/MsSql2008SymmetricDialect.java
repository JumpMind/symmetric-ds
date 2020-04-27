package org.jumpmind.symmetric.db.mssql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.IConnectionCallback;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.service.IParameterService;

public class MsSql2008SymmetricDialect extends MsSqlSymmetricDialect {
    public MsSql2008SymmetricDialect() {
        super();
    }

    public MsSql2008SymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerTemplate = new MsSql2008TriggerTemplate(this);
    }
    
    @Override
    protected String getDbSpecificDataHasChangedCondition(Trigger trigger) {
        /* gets filled/replaced by trigger template as it will compare by each column */
        return "$(anyColumnChanged)";
    }
    
    @Override
    public boolean doesDdlTriggerExist(final String catalogName, final String schema, final String triggerName) {
        return ((JdbcSqlTemplate) platform.getSqlTemplate()).execute(new IConnectionCallback<Boolean>() {
            public Boolean execute(Connection con) throws SQLException {
                String previousCatalog = con.getCatalog();
                PreparedStatement stmt = con.prepareStatement("select count(*) from sys.triggers where name = ?");
                try {
                    if (catalogName != null) {
                        con.setCatalog(catalogName);
                    }
                    stmt.setString(1, triggerName);
                    ResultSet rs = stmt.executeQuery();
                    if (rs.next()) {
                        int count = rs.getInt(1);
                        return count > 0;
                    }
                } finally {
                    if (catalogName != null) {
                        con.setCatalog(previousCatalog);
                    }
                    stmt.close();
                }
                return Boolean.FALSE;
            }
        });
    }

}
