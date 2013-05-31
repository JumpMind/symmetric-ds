package org.jumpmind.symmetric.web;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.service.IParameterService;

/**
 * Simple handler that returns a 200 to indicate that SymmetricDS is deployed
 * and running.
 */
public class PingUriHandler extends AbstractUriHandler {

    public PingUriHandler(IParameterService parameterService) {
        super("/ping/*", parameterService);
    }

    public void handle(HttpServletRequest req, HttpServletResponse res) throws IOException,
            ServletException {
    }

}