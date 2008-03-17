/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Henglin Wang <henglinwang@users.sourceforge.net>
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
import org.apache.log4j.Logger;

/*
 * 
 * <?xml version="1.0" encoding="UTF-8"?>
 <web-app id="WebApp_ID" version="2.4" xmlns="http://java.sun.com/xml/ns/j2ee" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://java.sun.com/xml/ns/j2ee http://java.sun.com/xml/ns/j2ee/web-app_2_4.xsd">
 <display-name>
 testFilter</display-name>
 <servlet>
 <description>
 </description>
 <display-name>
 TestServlet</display-name>
 <servlet-name>TestServlet</servlet-name>
 <servlet-class>
 org.jumpmind.symmetric.web.TestServlet</servlet-class>
 </servlet>
 <servlet-mapping>
 <servlet-name>TestServlet</servlet-name>
 <url-pattern>/TestServlet</url-pattern>
 </servlet-mapping>
 <welcome-file-list>
 <welcome-file>index.html</welcome-file>
 <welcome-file>index.htm</welcome-file>
 <welcome-file>index.jsp</welcome-file>
 <welcome-file>default.html</welcome-file>
 <welcome-file>default.htm</welcome-file>
 <welcome-file>default.jsp</welcome-file>
 </welcome-file-list>
 <filter>
 <filter-name>ThrottleFilter</filter-name>
 <filter-class>org.jumpmind.symmetric.web.ThrottleFilter</filter-class>
 <init-param>
 <param-name>maxBps</param-name>
 <param-value>10240</param-value>
 </init-param>
 <init-param>
 <param-name>threshold</param-name>
 <param-value>8192</param-value>
 </init-param>
 <init-param>
 <param-name>checkPoint</param-name>
 <param-value>4096</param-value>
 </init-param>
 </filter>
 <filter-mapping>
 <filter-name>ThrottleFilter</filter-name>
 <url-pattern>/TestServlet</url-pattern>
 </filter-mapping>
 </web-app>
 * 
 */

public class ThrottleFilter extends AbstractFilter
{

    private static Logger logger = Logger.getLogger(ThrottleFilter.class);

    private long maxBps;

    private long threshold;

    private long checkPoint;

    // default threshold before throttling in number of bytes
    private static final long DEFFAULT_THRESHOLD = 8192L;

    // default frequency to recalculation rate in number of bytes
    private static final long DEFAULT_CHECK_POINT = 1024L;

    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
    throws IOException, ServletException
    {
        ThrottledResponseWrapper wrapper = new ThrottledResponseWrapper((HttpServletResponse) response);
        wrapper.setCheckPoint((Long)ObjectUtils.defaultIfNull(checkPoint, DEFAULT_CHECK_POINT));
        wrapper.setMaxBps(maxBps);
        wrapper.setThreshold((Long)ObjectUtils.defaultIfNull(threshold, DEFFAULT_THRESHOLD));
        if (logger.isDebugEnabled())
        {
            logger.debug("Before hit servlet");
        }
        chain.doFilter(request, wrapper);

        if (logger.isDebugEnabled())
        {
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

}
