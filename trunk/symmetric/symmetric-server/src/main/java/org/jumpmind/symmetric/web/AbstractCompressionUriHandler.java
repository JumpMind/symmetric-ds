package org.jumpmind.symmetric.web;

import java.io.IOException;
import java.util.Enumeration;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jumpmind.log.Log;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.web.compression.CompressionServletResponseWrapper;

abstract public class AbstractCompressionUriHandler extends AbstractUriHandler {

    public AbstractCompressionUriHandler(Log log, String uriPattern,
            IParameterService parameterService, IInterceptor... interceptors) {
        super(log, uriPattern, parameterService, interceptors);
    }

    final public void handle(HttpServletRequest req, HttpServletResponse res) throws IOException,
            ServletException {
        boolean compressionEnabled = !parameterService
                .is(ParameterConstants.TRANSPORT_HTTP_COMPRESSION_DISABLED_SERVLET);
        if (compressionEnabled) {
            int compressionLevel = parameterService
                    .getInt(ParameterConstants.TRANSPORT_HTTP_COMPRESSION_LEVEL);
            int compressionStrategy = parameterService
                    .getInt(ParameterConstants.TRANSPORT_HTTP_COMPRESSION_STRATEGY);
            log.debug("CompressionFilterStarting");

            boolean supportCompression = false;
            log.debug("CompressionFilterURI", req.getRequestURI());

            // Are we allowed to compress ?
            String s = (String) req.getParameter("gzip");
            if ("false".equals(s)) {
                log.debug("CompressionFilterNotCompressing");
                handleWithCompression(req, res);
                return;
            }

            @SuppressWarnings("rawtypes")
            Enumeration e = req.getHeaders("Accept-Encoding");
            while (e.hasMoreElements()) {
                String name = (String) e.nextElement();
                if (name.indexOf("gzip") != -1) {
                    log.debug("CompressionFilterSupportsCompression");
                    supportCompression = true;
                } else {
                    log.debug("CompressionFilterDoesNotSupportsCompression");
                }
            }

            if (!supportCompression) {
                log.debug("CompressionFilterCalledNotCompressing");
                handleWithCompression(req, res);
                return;
            } else {
                CompressionServletResponseWrapper wrappedResponse = new CompressionServletResponseWrapper(
                        res, compressionLevel, compressionStrategy);
                log.debug("CompressionFilterCalledCompressing");
                try {
                    handleWithCompression(req, wrappedResponse);
                } finally {
                    wrappedResponse.finishResponse();
                }
                return;
            }
        } else {
            handleWithCompression(req, res);
        }

    }

    abstract protected void handleWithCompression(HttpServletRequest req, HttpServletResponse res)
            throws IOException, ServletException;

}
