/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */

package org.jumpmind.symmetric.web;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.ObjectUtils;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;

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
 *
 * ,
 */

public class ThrottleFilter extends AbstractFilter implements IBuiltInExtensionPoint {

    private final static ILog log = LogFactory.getLog(ThrottleFilter.class);

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
        log.debug("ThrottleFilterStarting");
        chain.doFilter(request, wrapper);
        log.info("ThrottleFilterCompleted");
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
    protected ILog getLog() {
        return log;
    }

}