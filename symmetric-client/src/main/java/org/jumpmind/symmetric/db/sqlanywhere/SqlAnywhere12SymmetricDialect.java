package org.jumpmind.symmetric.db.sqlanywhere;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.db.mssql.MsSql2008TriggerTemplate;
import org.jumpmind.symmetric.service.IParameterService;

public class SqlAnywhere12SymmetricDialect extends SqlAnywhereSymmetricDialect {

    public SqlAnywhere12SymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerTemplate = new SqlAnywhere12TriggerTemplate(this);
    }
}
