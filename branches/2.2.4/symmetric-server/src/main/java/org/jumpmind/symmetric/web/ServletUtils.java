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

import javax.servlet.ServletContext;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.StandaloneSymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.service.IParameterService;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * 
 */
public class ServletUtils {
    /**
     * Because you can't send an error when the response is already committed,
     * this helps to avoid unnecessary errors in the logs.
     * 
     * @param resp
     * @param statusCode
     * @return true if the error could be sent to the response
     * @throws IOException
     */
    public static boolean sendError(final HttpServletResponse resp, final int statusCode) throws IOException {
        return sendError(resp, statusCode, null);
    }

    /**
     * Because you can't send an error when the response is already committed,
     * this helps to avoid unnecessary errors in the logs.
     * 
     * @param resp
     * @param statusCode
     * @param message
     *            a message to put in the body of the response
     * @return true if the error could be sent to the response
     * @throws IOException
     */
    public static boolean sendError(final HttpServletResponse resp, final int statusCode, final String message)
            throws IOException {
        boolean retVal = false;
        if (!resp.isCommitted()) {
            resp.sendError(statusCode, message);
            retVal = true;
        }
        return retVal;
    }

    /**
     * Because you can't send an error when the response is already committed,
     * this helps to avoid unnecessary errors in the logs.
     * 
     * @param resp
     * @param statusCode
     * @return true if the error could be sent to the response
     * @throws IOException
     */
    public static boolean sendError(final ServletResponse resp, final int statusCode) throws IOException {
        return sendError(resp, statusCode, null);
    }

    /**
     * Because you can't send an error when the response is already committed,
     * this helps to avoid unnecessary errors in the logs.
     * 
     * @param resp
     * @param statusCode
     * @param message
     *            a message to put in the body of the response
     * @return true if the error could be sent to the response
     * @throws IOException
     */
    public static boolean sendError(final ServletResponse resp, final int statusCode, final String message)
            throws IOException {
        boolean retVal = false;
        if (resp instanceof HttpServletResponse) {
            retVal = sendError((HttpServletResponse) resp, statusCode, message);
        }
        return retVal;
    }

    /**
     * Search in several places for an {@link ApplicationContext} that contains
     * SymmetricDS services. This method uses existence of
     * {@link IParameterService} in the context as a clue as to if the context
     * contains SymmetricDS artifacts.
     */
    public static ApplicationContext getApplicationContext(ServletContext servletContext) {
        ApplicationContext ctx = WebApplicationContextUtils.getWebApplicationContext(servletContext);
        if (!(ctx.containsBean(Constants.PARAMETER_SERVICE) && ctx.getBean(Constants.PARAMETER_SERVICE) instanceof IParameterService)) {
            ISymmetricEngine engine = null;
            if (ctx != null && ctx.containsBean(Constants.SYMMETRIC_ENGINE)) {
                engine = (ISymmetricEngine) ctx.getBean(Constants.SYMMETRIC_ENGINE);
            } else {
                engine = StandaloneSymmetricEngine.getEngine();
            }
            if (engine != null) {
                ctx = engine.getApplicationContext();
            }
        }
        return ctx;
    }
}