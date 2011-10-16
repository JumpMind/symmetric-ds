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

import javax.servlet.Servlet;

/**
 * This is an extension that an be used to register a Servlet that gets called by
 * SymmetricDS's {@link SymmetricServlet} 
 *
 * 
 */
public class ServletExtension implements IServletExtension {

    boolean autoRegister = true;

    Servlet servlet;

    String[] uriPatterns;

    int initOrder = 0;

    boolean disabled = false;

    public boolean isAutoRegister() {
        return autoRegister;
    }

    public void setInitOrder(int initOrder) {
        this.initOrder = initOrder;
    }

    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }

    public void setServlet(Servlet servlet) {
        this.servlet = servlet;
    }

    public void setUriPatterns(String[] uriPatterns) {
        this.uriPatterns = uriPatterns;
    }
    
    public void setUriPattern(String uriPattern) {
        this.uriPatterns = new String[] { uriPattern };
    }

    public Servlet getServlet() {
        return this.servlet;
    }

    public int getInitOrder() {
        return this.initOrder;
    }

    public String[] getUriPatterns() {
        return this.uriPatterns;
    }

    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }

    public boolean isDisabled() {
        return this.disabled;
    }
}