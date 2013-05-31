
package org.jumpmind.symmetric.model;

/**
 * {@link TriggerHistory}
 */
public enum TriggerReBuildReason {

    NEW_TRIGGERS("N"), TABLE_SCHEMA_CHANGED("S"), TABLE_SYNC_CONFIGURATION_CHANGED("C"), FORCED("F"), TRIGGERS_MISSING(
            "T");

    private String code;

    TriggerReBuildReason(String code) {
        this.code = code;
    }

    public String getCode() {
        return this.code;
    }

    public static TriggerReBuildReason fromCode(String code) {
        if (code != null && code.length() > 0) {
            if (code.equals(NEW_TRIGGERS.code)) {
                return NEW_TRIGGERS;
            } else if (code.equals(TABLE_SCHEMA_CHANGED.code)) {
                return TABLE_SCHEMA_CHANGED;
            } else if (code.equals(TABLE_SYNC_CONFIGURATION_CHANGED.code)) {
                return TABLE_SYNC_CONFIGURATION_CHANGED;
            } else if (code.equals(FORCED.code)) {
                return FORCED;
            }
        }
        return null;
    }

}