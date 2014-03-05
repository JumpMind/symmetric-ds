package org.jumpmind.symmetric.db.db2;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.service.IParameterService;

public class Db2zOsSymmetricDialect extends Db2SymmetricDialect implements ISymmetricDialect {

    public Db2zOsSymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
    }

    // TODO: add check to trigger template if CURRENT SQLID = '${db.user}'
    //public String getSyncTriggersExpression() {
    //    return "CURRENT SQLID = ";
    //}

}
