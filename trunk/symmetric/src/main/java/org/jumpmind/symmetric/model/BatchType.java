package org.jumpmind.symmetric.model;

import org.jumpmind.symmetric.util.ICoded;

public enum BatchType implements ICoded {

    EVENTS("EV"), INITIAL_LOAD("IL");

    private String code;

    BatchType(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

}
