package org.jumpmind.symmetric.ext;

import java.util.ArrayList;
import java.util.List;

public class DefaultExtraConfigTables implements IExtraConfigTables {

    private boolean autoRegister = true;

    private List<String> tables;

    public boolean isAutoRegister() {
        return autoRegister;
    }

    public List<String> provideTableNames() {
        if (tables == null) {
            tables = new ArrayList<String>(0);
        }
        return tables;
    }

    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }

}
