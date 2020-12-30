package org.jumpmind.symmetric.db.mssql;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.service.IParameterService;

public class MsSql2016SymmetricDialect extends MsSql2008SymmetricDialect {
    
    public MsSql2016SymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerTemplate = new MsSql2016TriggerTemplate(this);
    }
}
