package org.jumpmind.symmetric.web.rest;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus( value = HttpStatus.INTERNAL_SERVER_ERROR )
public class InternalServerErrorException extends RuntimeException {

    private static final long serialVersionUID = 1L;

}
