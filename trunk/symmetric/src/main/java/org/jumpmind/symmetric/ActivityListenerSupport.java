package org.jumpmind.symmetric;

import org.jumpmind.symmetric.db.IDbDialect;

public class ActivityListenerSupport implements IActivityListener {

    public boolean createConfigurationTables(IDbDialect dbDialect) {
        return true;
    }

    public boolean upgradeNeeded(String oldVersion, String newVersion) {
        return false;
    }

    public void dataBatchReceived(DataEvent event) {
    }

    public void dataBatchSent(DataEvent event) {
    }

    public void loaded() {
    }

    public void nodeRegistered(String nodeId) {
    }

    public void registered() {
    }

    public void registrationOpened(String nodeId) {
    }

    public void tablesCreated() {
    }

}
