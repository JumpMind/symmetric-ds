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
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.statistic.StatisticName;

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

    private final static Log logger = LogFactory.getLog(NodeConcurrencyFilter.class);

    protected long waitTimeBetweenRetriesInMs = 500;

    private static int tooBusyCount;

    private IParameterService parameterService;
    
    private IStatisticManager statisticManager;

    private static long lastTooBusyLogTime = System.currentTimeMillis();

    static Map<String, Integer> numberOfWorkersByServlet = new HashMap<String, Integer>();

    @Override
    public boolean isContainerCompatible() {
        return true;
    }

    public void doFilter(final ServletRequest req, final ServletResponse resp, final FilterChain chain)
            throws IOException, ServletException {
        String servletPath = ((HttpServletRequest) req).getServletPath();
        if (!doWork(servletPath, new IWorker() {
            public void work() throws ServletException, IOException {
                chain.doFilter(req, resp);
            }
        })) {
            sendError(resp, HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        }

    }

    protected boolean doWork(String servletPath, IWorker worker) throws ServletException, IOException {
        boolean didWork = false;
        int tries = 5;
        int numberOfWorkers;
        do {
            numberOfWorkers = getNumberOfWorkers(servletPath);
            if (numberOfWorkers < parameterService.getInt(ParameterConstants.CONCURRENT_WORKERS)) {
                try {
                    changeNumberOfWorkers(servletPath, 1);
                    worker.work();
                    statisticManager.getStatistic(StatisticName.NODE_CONCURRENCY_FILTER_DID_WORK_COUNT).increment();
                    didWork = true;
                } finally {
                    changeNumberOfWorkers(servletPath, -1);
                }
            } else {
                if (--tries == 0) {
                    tooBusyCount++;
                    statisticManager.getStatistic(StatisticName.NODE_CONCURRENCY_FILTER_TOO_BUSY_COUNT).increment();
                    if ((System.currentTimeMillis() - lastTooBusyLogTime) > DateUtils.MILLIS_PER_MINUTE
                            * TOO_BUSY_LOG_STATEMENTS_PER_MIN
                            && tooBusyCount > 0) {
                        logger.warn(tooBusyCount + " symmetric requests were rejected in the last "
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
        } while (numberOfWorkers >= parameterService.getInt(ParameterConstants.CONCURRENT_WORKERS)
                && tries > 0);
        return didWork;
    }

    private int getNumberOfWorkers(String servletPath) {
        Integer number = numberOfWorkersByServlet.get(servletPath);
        return number == null ? 0 : number;
    }

    synchronized private void changeNumberOfWorkers(String servletPath, int delta) {
        numberOfWorkersByServlet.put(servletPath, getNumberOfWorkers(servletPath) + delta);
    }

    interface IWorker {
        public void work() throws ServletException, IOException;
    }

    @Override
    protected Log getLogger() {
        return logger;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    public void setStatisticManager(IStatisticManager statisticManager) {
        this.statisticManager = statisticManager;
    }
}
