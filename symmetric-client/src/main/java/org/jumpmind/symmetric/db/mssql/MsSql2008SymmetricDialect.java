package org.jumpmind.symmetric.db.mssql;

import org.jumpmind.db.platform.IDatabasePlatform;
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
}
