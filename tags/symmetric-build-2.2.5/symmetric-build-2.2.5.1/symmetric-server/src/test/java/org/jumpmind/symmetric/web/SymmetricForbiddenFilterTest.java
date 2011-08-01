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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.test.TestSetupUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockFilterConfig;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

/**
 * This simply makes sure the SymmetricFilter is setup correctly.
 */
@RunWith(Parameterized.class)
public class SymmetricForbiddenFilterTest extends AbstractSymmetricFilterTest {


    public SymmetricForbiddenFilterTest(String method, String uri, Map<String, String> parameters) throws Exception {
        super(method, uri, parameters);
    }

    @Parameters
    public static Collection<Object[]> authenticationFilterForbiddenParams() {
        final Map<String, String> emptyAuthentication = new HashMap<String, String>();
        emptyAuthentication.put(WebConstants.SECURITY_TOKEN, "");
        emptyAuthentication.put(WebConstants.NODE_ID, "");

        return Arrays.asList(new Object[][] { { "GET", "/sync/ack", null }, { "GET", "/sync/ack/", null },
                { "GET", "/sync/ack/more", null }, { "GET", "/sync/ack?name=value", null },
                { "GET", "/sync/ack?name=value&name=value", null },
                { "GET", String.format("/sync/ack?%s=1&%s=2", WebConstants.SECURITY_TOKEN, WebConstants.NODE_ID), null },
                { "GET", "/sync/ack", emptyAuthentication }, { "PUT", "/sync/ack", null }, { "POST", "/sync/ack", null },
                { "DELETE", "/sync/ack", null }, { "TRACE", "/sync/ack", null }, { "OPTIONS", "/sync/ack", null },
                { "HEAD", "/sync/ack", null }, { "GET", "/sync/pull", null }, { "GET", "/sync/pull/", null },
                { "GET", "/sync/pull/more", null }, { "GET", "/sync/pull?name=value", null },
                { "GET", "/sync/pull?name=value&name=value", null },
                { "GET", String.format("/sync/pull?%s=1&%s=2", WebConstants.SECURITY_TOKEN, WebConstants.NODE_ID), null },
                { "GET", "/sync/pull", emptyAuthentication }, { "PUT", "/sync/pull", null }, { "POST", "/sync/pull", null },
                { "DELETE", "/sync/pull", null }, { "TRACE", "/sync/pull", null }, { "OPTIONS", "/sync/pull", null },
                { "HEAD", "/sync/pull", null }, { "GET", "/sync/push", null }, { "GET", "/sync/push/", null },
                { "GET", "/sync/push/more", null }, { "GET", "/sync/push?name=value", null },
                { "GET", "/sync/push?name=value&name=value", null },
                { "GET", String.format("/sync/push?%s=1&%s=2", WebConstants.SECURITY_TOKEN, WebConstants.NODE_ID), null },
                { "GET", "/sync/push", emptyAuthentication }, { "PUT", "/sync/push", null }, { "POST", "/sync/push", null },
                { "DELETE", "/sync/push", null }, { "TRACE", "/sync/push", null }, { "OPTIONS", "/sync/push", null },
                { "HEAD", "/sync/push", null }, });
    }

    @Test
    public void testAuthenticationFilterForbidden()
            throws Exception {
        final SymmetricFilter filter = new SymmetricFilter();
        filter.init(new MockFilterConfig(servletContext));
        final MockHttpServletRequest request = TestSetupUtil.createMockHttpServletRequest(servletContext, method, uri, parameters);
        final MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, new MockFilterChain());
        Assert.assertEquals(response.getStatus(), HttpServletResponse.SC_FORBIDDEN);
        filter.destroy();
    }

}