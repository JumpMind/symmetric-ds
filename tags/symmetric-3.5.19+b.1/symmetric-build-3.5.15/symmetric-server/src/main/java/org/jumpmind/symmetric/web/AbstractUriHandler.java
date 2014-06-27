/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.web;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.transport.internal.InternalOutgoingTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class AbstractUriHandler implements IUriHandler {
    
    protected final Logger log = LoggerFactory.getLogger(getClass());
    
    private String uriPattern;
    
    private List<IInterceptor> interceptors;
    
    protected IParameterService parameterService;
    
    private boolean enabled = true;
    
    public AbstractUriHandler(String uriPattern, IParameterService parameterService, 
            IInterceptor... interceptors) {
        this.uriPattern = uriPattern;
        this.interceptors = new ArrayList<IInterceptor>(interceptors.length);
        for (IInterceptor i : interceptors) {
            this.interceptors.add(i);
        }
        this.parameterService = parameterService;
    }

    public void setUriPattern(String uriPattern) {
        this.uriPattern = uriPattern;
    }
    
    public String getUriPattern() {
        return uriPattern;
    }

    public void setInterceptors(List<IInterceptor> interceptors) {
        this.interceptors = interceptors;
    }
    
    public List<IInterceptor> getInterceptors() {
        return interceptors;
    }
    
    protected IOutgoingTransport createOutgoingTransport(OutputStream outputStream, String encoding, ChannelMap map) throws IOException {
        return new InternalOutgoingTransport(outputStream, map, encoding);
    }

    protected IOutgoingTransport createOutgoingTransport(OutputStream outputStream, String encoding) throws IOException {
        return new InternalOutgoingTransport(outputStream, new ChannelMap(), encoding);
    }
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    
    public boolean isEnabled() {
        return enabled;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }
}
