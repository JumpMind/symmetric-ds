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
import java.util.HashSet;
import java.util.Set;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;

public class HttpMethodFilter implements Filter {
    
    private Set<String> allowedMethods = new HashSet<String>();
    private Set<String> disallowedMethods = new HashSet<String>();

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        String allowMethodsConfig = filterConfig.getInitParameter("server.allow.http.methods"); 
        loadMethods(allowMethodsConfig, allowedMethods);
        String disallowMethodsConfig = filterConfig.getInitParameter("server.disallow.http.methods");
        loadMethods(disallowMethodsConfig, disallowedMethods);
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
            FilterChain filterChain) throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        String method = httpRequest.getMethod().toUpperCase();
        
        if (disallowedMethods.contains(method))  {
            forbid(method, request, response);
        } else if (!allowedMethods.isEmpty() && !allowedMethods.contains(method)) {
            forbid(method, request, response);
        } else {            
            filterChain.doFilter(request, response);
        }
    }

    protected void forbid(String method, ServletRequest request, ServletResponse response) throws IOException {
        HttpServletResponse httpResponse = (HttpServletResponse)response;
        httpResponse.sendError(HttpServletResponse.SC_FORBIDDEN, "Method " + method + " is not allowed.");
    }

    protected void loadMethods(String configuredValue, Set<String> methods) {
        if (!StringUtils.isEmpty(configuredValue)) {
            String[] methodsSplit = configuredValue.split(",");
            for (String method : methodsSplit) {
                if (!StringUtils.isEmpty(method)) {
                    methods.add(method.toUpperCase());                    
                }
            }
        }        
    }

    @Override
    public void destroy() {
        // Empty.
    }

}
