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

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.transport.ITransportResource;
import org.springframework.beans.BeanUtils;

/**
 * All symmetric filters (other than {@link SymmetricFilter}) should extend this
 * class. It is managed by Spring.
 * 
 * @since 1.4.0
 */
public abstract class AbstractFilter extends ServletResourceTemplate implements IServletFilterExtension {

    final ILog log = LogFactory.getLog(getClass());
    
    protected ILog getLog() {
       return log;    
    }

    public void init(FilterConfig filterConfig) throws ServletException {
        init(filterConfig.getServletContext());
        if (isContainerCompatible() && !this.isSpringManaged()) {
            final IServletResource springBean = getSpringBean();
            if (this != springBean) { // this != is deliberate!
                if (getLog().isInfoEnabled()) {
                    getLog().info(String.format("Initializing filter %s", springBean.getClass().getSimpleName()));
                }
                BeanUtils.copyProperties(springBean, this, IServletResource.class);
                BeanUtils.copyProperties(springBean, this, ITransportResource.class);
                BeanUtils.copyProperties(springBean, this, this.getClass());
            }
        }
    }

    public boolean isAutoRegister() {
        return true;
    }
}