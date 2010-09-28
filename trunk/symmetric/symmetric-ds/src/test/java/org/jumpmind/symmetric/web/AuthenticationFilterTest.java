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
 *
 * @author Keith Naas <knaas@users.sourceforge.net>
 */
@RunWith(Parameterized.class)
public class AuthenticationFilterTest extends AbstractSymmetricFilterTest {

    public AuthenticationFilterTest(String method, String uri, Map<String, String> parameters) throws Exception {
        super(method, uri, parameters);
    }

    @Parameters
    public static Collection<Object[]> authenticationFilterParams() {
        final Map<String, String> goodAuthentication = new HashMap<String, String>();
        goodAuthentication.put(WebConstants.SECURITY_TOKEN, MockNodeService.GOOD_SECURITY_TOKEN);
        goodAuthentication.put(WebConstants.NODE_ID, MockNodeService.GOOD_NODE_ID);
        return Arrays.asList(new Object[][] { { "GET", "/ack", goodAuthentication }, { "POST", "/ack", goodAuthentication },
                { "PUT", "/ack", goodAuthentication }, { "DELETE", "/ack", goodAuthentication },
                { "TRACE", "/ack", goodAuthentication }, { "OPTIONS", "/ack", goodAuthentication },
                { "HEAD", "/ack", goodAuthentication }, { "GET", "/pull", goodAuthentication },
                { "POST", "/pull", goodAuthentication }, { "PUT", "/pull", goodAuthentication },
                { "DELETE", "/pull", goodAuthentication }, { "TRACE", "/pull", goodAuthentication },
                { "OPTIONS", "/pull", goodAuthentication }, { "HEAD", "/pull", goodAuthentication },
                { "GET", "/push", goodAuthentication }, { "POST", "/push", goodAuthentication },
                { "PUT", "/push", goodAuthentication }, { "DELETE", "/push", goodAuthentication },
                { "TRACE", "/push", goodAuthentication }, { "OPTIONS", "/push", goodAuthentication },
                { "HEAD", "/push", goodAuthentication } });
    }

    @Test
    public void testAuthenticationFilter() throws Exception {
        final SymmetricFilter filter = new SymmetricFilter();
        filter.init(new MockFilterConfig(servletContext));
        final MockHttpServletRequest request = TestSetupUtil.createMockHttpServletRequest(servletContext, method, uri, parameters);
        final MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, new MockFilterChain());
        Assert.assertEquals(HttpServletResponse.SC_FORBIDDEN, response.getStatus());
        filter.destroy();
    }

}