package org.jumpmind.symmetric.web;

import java.io.IOException;
import java.util.List;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.time.DateUtils;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.model.Node;

/**
 * The chain will visit each filter in turn. When done, it will pass along to
 * the original chain. The chain skips disabled filters. I'm wondering if this
 * should be moved to the {@link SymmetricFilter#init(FilterConfig)}.
 */
public class SymmetricFilterChain implements FilterChain {

    private static final ILog log = LogFactory.getLog(SymmetricFilterChain.class);
    private FilterChain chain;
    private int index;
    private List<Filter> filters;
    private Node identity;

    private static long lastWarningTimestamp = 0;

    public SymmetricFilterChain(FilterChain chain, List<Filter> filters, Node identity) {
        this.identity = identity;
        this.filters = filters;
        this.chain = chain;
        this.index = 0;
    }

    public void doFilter(ServletRequest request, ServletResponse response) throws IOException,
            ServletException {
        if (!response.isCommitted()) {
            if (identity != null) {
                if (identity.isSyncEnabled()) {
                    if (index < filters.size()) {
                        final Filter filter = filters.get(index++);
                        if (filter instanceof AbstractFilter) {
                            final AbstractFilter builtinFilter = (AbstractFilter) filter;
                            if (!builtinFilter.isDisabled() && builtinFilter.matches(request)) {
                                builtinFilter.doFilter(request, response, this);
                            } else {
                                this.doFilter(request, response);
                            }
                        } else {
                            filter.doFilter(request, response, this);
                        }
                    } else {
                        chain.doFilter(request, response);
                    }
                } else {
                    if (System.currentTimeMillis() - lastWarningTimestamp > DateUtils.MILLIS_PER_MINUTE) {
                        log.warn("NodeDisableNotAcceptingWebRequests");
                        lastWarningTimestamp = System.currentTimeMillis();
                    }
                    ServletUtils.sendError((HttpServletResponse) response,
                            WebConstants.SC_FORBIDDEN);
                }
            } else {
                if (System.currentTimeMillis() - lastWarningTimestamp > DateUtils.MILLIS_PER_MINUTE) {
                    log.warn("NodeNotConfiguredNotAcceptingWebRequests");
                    lastWarningTimestamp = System.currentTimeMillis();
                }
                ServletUtils.sendError((HttpServletResponse) response, WebConstants.SC_FORBIDDEN);
            }
        }
    }
}
