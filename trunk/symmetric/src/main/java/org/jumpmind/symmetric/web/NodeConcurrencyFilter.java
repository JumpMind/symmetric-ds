package org.jumpmind.symmetric.web;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.Constants;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

public class NodeConcurrencyFilter implements Filter {

    final static Log logger = LogFactory.getLog(NodeConcurrencyFilter.class);

    private ServletContext context;

    public void destroy() {
    }

    static int numberOfWorkers;

    public void doFilter(ServletRequest req, ServletResponse resp,
            FilterChain chain) throws IOException, ServletException {
        try {
            numberOfWorkers++;
            if (numberOfWorkers >= getMaxNumberOfWorkers()) {
                ((HttpServletResponse) resp)
                        .sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
                logger
                        .error("Symmetric request was rejected because the server was too busy.");
            } else {
                chain.doFilter(req, resp);
            }
        } finally {
            numberOfWorkers--;
        }
    }

    public void init(FilterConfig config) throws ServletException {
        context = config.getServletContext();
    }

    private int getMaxNumberOfWorkers() {
        ApplicationContext ctx = WebApplicationContextUtils
                .getWebApplicationContext(context);
        return (Integer) ctx.getBean(Constants.MAX_CONCURRENT_WORKERS);
    }

}
