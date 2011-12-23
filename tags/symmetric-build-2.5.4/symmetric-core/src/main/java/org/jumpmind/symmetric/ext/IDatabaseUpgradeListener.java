package org.jumpmind.symmetric.ext;

import java.io.IOException;

import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.ddl.model.Database;

public interface IDatabaseUpgradeListener extends IExtensionPoint {

    public String beforeUpgrade(IDbDialect dbDialect, String tablePrefix, Database currentModel,
        Database desiredModel) throws IOException;

    public String afterUpgrade(IDbDialect dbDialect, String tablePrefix, Database model) throws IOException;

}
