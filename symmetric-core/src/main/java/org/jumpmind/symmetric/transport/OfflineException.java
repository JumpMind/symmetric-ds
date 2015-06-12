package org.jumpmind.symmetric.transport;

import org.jumpmind.exception.IoException;

public class OfflineException extends IoException {

    private static final long serialVersionUID = 1L;

    public OfflineException() {
        super();
    }

    public OfflineException(Exception e) {
        super(e);
    }

    public OfflineException(String msg, Object... args) {
        super(msg, args);
    }

}
