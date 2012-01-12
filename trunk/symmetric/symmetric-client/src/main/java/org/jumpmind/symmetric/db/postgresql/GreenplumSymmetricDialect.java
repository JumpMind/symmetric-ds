package org.jumpmind.symmetric.db.postgresql;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.log.Log;
import org.jumpmind.symmetric.service.IParameterService;

public class GreenplumSymmetricDialect extends PostgreSqlSymmetricDialect {

    public GreenplumSymmetricDialect(Log log, IParameterService parameterService, IDatabasePlatform platform) {
        super(log, parameterService, platform);
        this.triggerText = new GreenplumTriggerText();
    }
    
}
