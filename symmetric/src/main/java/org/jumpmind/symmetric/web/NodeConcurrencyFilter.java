/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Keith Naas <knaas@users.sourceforge.net>
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

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * Configured within symmetric-web.xml
 * 
 * <pre>
 *  &lt;bean id=&quot;nodeConcurrencyFilter&quot;
 *  class=&quot;org.jumpmind.symmetric.web.NodeConcurrencyFilter&quot;&gt;
 *    &lt;property name=&quot;regexPattern&quot; value=&quot;string&quot; /&gt;
 *    &lt;property name=&quot;regexPatterns&quot;&gt;
 *      &lt;list&gt;
 *        &lt;value value=&quot;string&quot;/&gt;
 *      &lt;list/&gt;
 *    &lt;property/&gt;
 *    &lt;property name=&quot;uriPattern&quot; value=&quot;string&quot; /&gt;
 *    &lt;property name=&quot;uriPatterns&quot;&gt;
 *      &lt;list&gt;
 *        &lt;value value=&quot;string&quot;/&gt;
 *      &lt;list/&gt;
 *    &lt;property/&gt;
 *    &lt;property name=&quot;disabled&quot; value=&quot;boolean&quot; /&gt;
 *    &lt;property name=&quot;maxNumberOfConcurrentWorkers&quot; value=&quot;int&quot; /&gt;
 *  &lt;/bean&gt;
 * </pre>
 */
public class NodeConcurrencyFilter extends AbstractFilter {

    private static final int TOO_BUSY_LOG_STATEMENTS_PER_MIN = 10;

    final static Log logger = LogFactory.getLog(NodeConcurrencyFilter.class);

    protected int maxNumberOfConcurrentWorkers = 20;

    protected long waitTimeBetweenRetriesInMs = 500;

    private static int tooBusyCount;

    private static long lastTooBusyLogTime = System.currentTimeMillis();

    static Map<String, Integer> numberOfWorkersByServlet = new HashMap<String, Integer>();

    public void doFilter(final ServletRequest req, final ServletResponse resp,
            final FilterChain chain) throws IOException, ServletException {
        String servletPath = ((HttpServletRequest) req).getServletPath();
        if (!doWork(servletPath, new IWorker() {
            public void work() throws ServletException, IOException {
                chain.doFilter(req, resp);
            }
        })) {
            sendError(resp, HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        }

    }

    protected boolean doWork(String servletPath, IWorker worker)
            throws ServletException, IOException {
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
            } else {
                if (--tries == 0) {
                    tooBusyCount++;

                    if ((System.currentTimeMillis() - lastTooBusyLogTime) > DateUtils.MILLIS_PER_MINUTE
                            * TOO_BUSY_LOG_STATEMENTS_PER_MIN
                            && tooBusyCount > 0) {
                        logger
                                .warn(tooBusyCount
                                        + " symmetric requests were rejected in the last "
                                        + TOO_BUSY_LOG_STATEMENTS_PER_MIN
                                        + " minutes because the server was too busy.");
                        lastTooBusyLogTime = System.currentTimeMillis();
                        tooBusyCount = 0;
                    }
                } else {
                    try {
                        Thread.sleep(waitTimeBetweenRetriesInMs);
                    } catch (InterruptedException ex) {
                    }
                }
            }
        } while (numberOfWorkers >= maxNumberOfConcurrentWorkers && tries > 0);
        return didWork;
    }

    private int getNumberOfWorkers(String servletPath) {
        Integer number = numberOfWorkersByServlet.get(servletPath);
        return number == null ? 0 : number;
    }

    synchronized private void changeNumberOfWorkers(String servletPath,
            int delta) {
        numberOfWorkersByServlet.put(servletPath,
                getNumberOfWorkers(servletPath) + delta);
    }

    interface IWorker {
        public void work() throws ServletException, IOException;
    }

    public void setMaxNumberOfConcurrentWorkers(int maxNumberOfConcurrentWorkers) {
        this.maxNumberOfConcurrentWorkers = maxNumberOfConcurrentWorkers;
    }

}
