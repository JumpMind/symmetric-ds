package org.jumpmind.symmetric.transport;

import java.io.IOException;

public class TransportException extends RuntimeException {

    private static final long serialVersionUID = -6127189404858972114L;

    public TransportException(IOException ex) {
        super(ex.getMessage(), ex);
    }

}