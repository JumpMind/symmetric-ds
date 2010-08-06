/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Henglin Wang <henglinwang@users.sourceforge.net>,
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

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * Configured within symmetric-web.xml
 * 
 * <pre>
 *  &lt;bean id=&quot;throttleFilter&quot;
 *  class=&quot;org.jumpmind.symmetric.web.ThrottleFilter&quot;&gt;
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
 *    &lt;property name=&quot;maxBps&quot; value=&quot;long&quot; /&gt;
 *    &lt;property name=&quot;threshold&quot; value=&quot;long&quot; /&gt;
 *    &lt;property name=&quot;checkPoint&quot; value=&quot;long&quot; /&gt;
 *  &lt;/bean&gt;
 * </pre>
 */

public class ThrottleFilter extends AbstractFilter {

    private final static Log logger = LogFactory.getLog(ThrottleFilter.class);

    private Long maxBps;

    private Long threshold;

    private Long checkPoint;

    // default threshold before throttling in number of bytes
    private static final long DEFAULT_THRESHOLD = 8192L;

    // default frequency to recalculation rate in number of bytes
    private static final long DEFAULT_CHECK_POINT = 1024L;

    @Override
    public boolean isContainerCompatible() {
        return true;
    }

    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException,
            ServletException {
        ThrottledResponseWrapper wrapper = new ThrottledResponseWrapper((HttpServletResponse) response);
        wrapper.setCheckPoint((Long) ObjectUtils.defaultIfNull(checkPoint, DEFAULT_CHECK_POINT));
        wrapper.setMaxBps((Long) ObjectUtils.defaultIfNull(maxBps, 0L));
        wrapper.setThreshold((Long) ObjectUtils.defaultIfNull(threshold, DEFAULT_THRESHOLD));
        if (logger.isDebugEnabled()) {
            logger.debug("Before hit servlet");
        }
        chain.doFilter(request, wrapper);

        if (logger.isDebugEnabled()) {
            logger.info("after hit servlet");
        }
    }

    public void setMaxBps(Long maxBps) {
        this.maxBps = maxBps;
    }

    public void setThreshold(Long threshold) {
        this.threshold = threshold;
    }

    public void setCheckPoint(Long checkPoint) {
        this.checkPoint = checkPoint;
    }

    public Long getMaxBps() {
        return maxBps;
    }

    public Long getThreshold() {
        return threshold;
    }

    public Long getCheckPoint() {
        return checkPoint;
    }

    @Override
    protected Log getLogger() {
        return logger;
    }

}
