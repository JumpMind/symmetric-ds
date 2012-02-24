package org.jumpmind.symmetric.db.postgresql;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.service.IParameterService;

public class GreenplumSymmetricDialect extends PostgreSqlSymmetricDialect {

    public GreenplumSymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerText = new GreenplumTriggerTemplate(this);
    }
    
}
