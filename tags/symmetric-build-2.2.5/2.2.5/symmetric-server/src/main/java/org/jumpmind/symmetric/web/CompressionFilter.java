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
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;

/**
 * 
 * Configured within symmetric-web.xml
 * 
 * <pre>
 *  &lt;bean id=&quot;compressionFilter&quot;
 *  class=&quot;org.jumpmind.symmetric.web.CompressionFilter&quot;&gt;
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
 *    &lt;property name=&quot;compressionThreshold&quot; value=&quot;int&quot; /&gt;
 *  &lt;/bean&gt;
 * </pre>
 *
 * ,
 */
public class CompressionFilter extends AbstractFilter 
  implements IBuiltInExtensionPoint {

    private static final ILog log = LogFactory.getLog(CompressionFilter.class);

    private org.jumpmind.symmetric.web.compression.CompressionFilter delegate;

    @Override
    public boolean isContainerCompatible() {
        return true;
    }

    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException,
            ServletException {
        if (delegate != null) {
            delegate.doFilter(request, response, chain);
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        if (delegate != null) {
            delegate.destroy();
        }
    }

    @Override
    public void init(final FilterConfig filterConfig) throws ServletException {
        super.init(filterConfig);
        delegate = new org.jumpmind.symmetric.web.compression.CompressionFilter();
        delegate.setCompressionLevel(parameterService.getInt(ParameterConstants.TRANSPORT_HTTP_COMPRESSION_LEVEL));
        delegate
                .setCompressionStrategy(parameterService.getInt(ParameterConstants.TRANSPORT_HTTP_COMPRESSION_STRATEGY));
        delegate.init(filterConfig);
    }

    @Override
    public boolean isDisabled() {
        return parameterService.is(ParameterConstants.TRANSPORT_HTTP_COMPRESSION_DISABLED_SERVLET);
    }

    @Override
    protected ILog getLog() {
        return log;
    }

}