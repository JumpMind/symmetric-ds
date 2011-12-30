package org.jumpmind.symmetric.ext;

import java.io.IOException;

import org.jumpmind.db.model.Database;
import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.db.ISymmetricDialect;

public interface IDatabaseUpgradeListener extends IExtensionPoint {

    public String beforeUpgrade(ISymmetricDialect symmetricDialect, String tablePrefix, Database currentModel,
        Database desiredModel) throws IOException;

    public String afterUpgrade(ISymmetricDialect symmetricDialect, String tablePrefix, Database model) throws IOException;

}
