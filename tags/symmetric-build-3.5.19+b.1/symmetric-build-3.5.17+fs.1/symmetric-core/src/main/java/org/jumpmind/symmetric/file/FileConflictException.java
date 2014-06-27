package org.jumpmind.symmetric.file;

import org.jumpmind.exception.IoException;

public class FileConflictException extends IoException {

    private static final long serialVersionUID = 1L;

    public FileConflictException(String msg) {
        super(msg);        
    }
    
    public FileConflictException() {
    }
}
