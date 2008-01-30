/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.web;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.Constants;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

public class NodeConcurrencyFilter implements Filter {

    private static final int TOO_BUSY_LOG_STATEMENTS_PER_MIN = 10;

    final static Log logger = LogFactory.getLog(NodeConcurrencyFilter.class);

    private ServletContext context;

    protected int maxNumberOfConcurrentWorkers = 20;

    protected long waitTimeBetweenRetriesInMs = 500;

    private static int tooBusyCount;

    private static long lastTooBusyLogTime = System.currentTimeMillis();

    public void destroy() {
    }

    static Map<String, Integer> numberOfWorkersByServlet = new HashMap<String, Integer>();

    public void doFilter(final ServletRequest req, final ServletResponse resp, final FilterChain chain)
            throws IOException, ServletException {
        final String servletPath = ((HttpServletRequest) req).getServletPath();
        if (!doWork(servletPath, new IWorker() {
            public void work() throws ServletException, IOException {
                chain.doFilter(req, resp);
            }
        })) {
            ((HttpServletResponse) resp).sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        }

    }

    protected boolean doWork(final String servletPath, final IWorker worker) throws ServletException, IOException {
        boolean didWork = false;
        int tries = 5;
        int numberOfWorkers;
        do {
            numberOfWorkers = getNumberOfWorkers(servletPath);
            if (numberOfWorkers < maxNumberOfConcurrentWorkers) {
                try {
                    changeNumberOfWorkers(servletPath, 1);
                    worker.work();
                    didWork = true;
                } finally {
                    changeNumberOfWorkers(servletPath, -1);
                }
            } else if (tries == 0) {
                tooBusyCount++;

                if ((System.currentTimeMillis() - lastTooBusyLogTime) > DateUtils.MILLIS_PER_MINUTE
                        * TOO_BUSY_LOG_STATEMENTS_PER_MIN
                        && tooBusyCount > 0) {
                    logger.warn(tooBusyCount + " symmetric requests were rejected in the last "
                            + TOO_BUSY_LOG_STATEMENTS_PER_MIN + " minutes because the server was too busy.");
                    tooBusyCount = 0;
                }
            } else {
                tries--;
                try {
                    Thread.sleep(waitTimeBetweenRetriesInMs);
                } catch (final InterruptedException ex) {
                }
            }
        } while (numberOfWorkers >= maxNumberOfConcurrentWorkers && tries > 0);
        return didWork;
    }

    private int getNumberOfWorkers(final String servletPath) {
        final Integer number = numberOfWorkersByServlet.get(servletPath);
        return number == null ? 0 : number;
    }

    synchronized private void changeNumberOfWorkers(final String servletPath, final int delta) {
        numberOfWorkersByServlet.put(servletPath, getNumberOfWorkers(servletPath) + delta);
    }

    public void init(final FilterConfig config) throws ServletException {
        context = config.getServletContext();
        final ApplicationContext ctx = WebApplicationContextUtils.getWebApplicationContext(context);
        maxNumberOfConcurrentWorkers = (Integer) ctx.getBean(Constants.MAX_CONCURRENT_WORKERS);
    }

    interface IWorker {
        public void work() throws ServletException, IOException;
    }

}
