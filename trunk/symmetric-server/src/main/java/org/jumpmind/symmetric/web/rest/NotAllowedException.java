package org.jumpmind.symmetric.web.rest;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value = HttpStatus.NOT_ACCEPTABLE)
public class NotAllowedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public NotAllowedException(String msg, Object... args) {
        super(String.format(msg, args));
    }

    public NotAllowedException() {

    }

}
