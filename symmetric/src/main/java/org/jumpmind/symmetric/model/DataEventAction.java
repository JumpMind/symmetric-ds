package org.jumpmind.symmetric.model;

import org.jumpmind.symmetric.util.ICoded;

/**
 * Identifies the action to take when the event watcher sees events in the event table.
 * @author chenson
 */
public enum DataEventAction implements ICoded {

    NOTIFY("N"), PUSH("P"), WAIT_FOR_POLL("W"), EXTRACT_LISTENER("X");

    private String code;

    DataEventAction(String code) {
        this.code = code;
    }

    public String getCode() {
        return this.code;
    }

    public static DataEventAction fromCode(String code) {
        if (code != null && code.length() > 0) {
            if (NOTIFY.code.equals(code)) {
                return NOTIFY;
            } else if (PUSH.code.equals(code)) {
                return PUSH;
            } else if (WAIT_FOR_POLL.code.equals(code)) {
                return WAIT_FOR_POLL;
            }
        }
        return null;
    }

}
