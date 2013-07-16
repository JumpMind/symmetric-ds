package org.jumpmind.db.platform.greenplum;

import org.jumpmind.db.platform.postgresql.PostgreSqlDdlBuilder;

public class GreenplumDdlBuilder extends PostgreSqlDdlBuilder {

    public GreenplumDdlBuilder() {
        databaseInfo.setTriggersSupported(false);
    }
}
