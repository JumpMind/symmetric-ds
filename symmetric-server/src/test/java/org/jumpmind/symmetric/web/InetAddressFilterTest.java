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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.test.AbstractWebTest;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockFilterConfig;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

/**
 * This simply makes sure the SymmetricFilter is setup correctly.
 *
 * 
 */
public class InetAddressFilterTest extends AbstractWebTest {
    public static final String[] HTTP_METHODS = { "GET", "POST", "DELETE", "PUT", "TRACE", "OPTIONS" };

    public static final String[] URIS = { "/sync/ack", "/sync/push", "/sync/pull" };

    public static final String COMBO_ADDRESS_FILTER = "105.100-10.5.15, 206.123.*.90, 184.*.209.210-99, 64-1.78.81.*, 164-120.*.181-50.*, 15-10.240-155.181-50.211-2";

    // public static final String COMBO_ADDRESS_FILTER = "105.100/10.5.15,
    // 206.123.*.90, 184.*.209.210/99, 64/1.78.81.*,
    // 164/120.*.181/50.*, 15/10.240/155.181/50.211/2";

    public static final String STATIC_ADDRESS_FILTER = "105.10.5.15, 206.123.45.90, 184.210.209.99, 64.78.81.11, 164.240.181.211";

    public static final String MULTICAST_ADDRESS_FILTER = "224.0.10.1, 239.254.254.254";

    public static final String CIDR_ADDRESS_FILTER = "10.10.1.32/27, 164.37.0.0/16";


    @Test
    public void testInetAddressFilterParameterServiceFilter() throws Exception {
        Object[][] params = parameterServiceCheckAddrParams();
        for (Object[] param : params) {
            testInetAddressFilterParameterServiceFilter((String) param[0]);
        }
    }
    
    private void testInetAddressFilterParameterServiceFilter(final String checkAddr) throws Exception {
        final InetAddressFilter filter = getFilter();
        final MockFilterConfig config = new MockFilterConfig(servletContext);
        filter.init(config);

        final MockHttpServletRequest request = createMockHttpServletRequest(checkAddr);
        final MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, new MockFilterChain());
        Assert.assertFalse(response.getStatus() == HttpServletResponse.SC_FORBIDDEN);

        filter.destroy();
    }
    
    private Object[][] parameterServiceCheckAddrParams() {
        return new Object[][] { { "10.4.201.179" }, { "10.1.200.4" }, { "10.4.100.43" }, { "11.100.121.83" },
                { "11.100.148.221" }, { "11.100.100.9" }, { "100.50.253.172" }, { "100.50.0.3" }, { "100.50.94.254" } };
    }

    @Test
    public void testInetAddressFilterCidrFilter() throws Exception {
        Object[][] params = cidrFilterParams();
        for (Object[] param : params) {
            testInetAddressFilterCidrFilter((String) param[0], (String) param[1]);
        }
    }

    private void testInetAddressFilterCidrFilter(final String filterString, final String checkAddr) throws Exception {
        final InetAddressFilter filter = getFilter();
        final MockFilterConfig config = new MockFilterConfig(servletContext);
        config.addInitParameter(InetAddressFilter.INET_ADDRESS_FILTERS, filterString);
        filter.init(config);

        final MockHttpServletRequest request = createMockHttpServletRequest(checkAddr);
        final MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, new MockFilterChain());
        Assert.assertFalse(response.getStatus() == HttpServletResponse.SC_FORBIDDEN);

        filter.destroy();
    }

    private Object[][] cidrFilterParams() {
        return new Object[][] { { CIDR_ADDRESS_FILTER, "10.10.1.44" }, { CIDR_ADDRESS_FILTER, "10.10.1.47" },
                { CIDR_ADDRESS_FILTER, "10.10.1.63" }, { CIDR_ADDRESS_FILTER, "164.37.99.251" },
                { CIDR_ADDRESS_FILTER, "164.37.2.59" } };
    }

    @Test
    public void testInetAddressFilterInvalidCidrFilter() throws Exception {
        Object[][] params = invalidCidrFilterParams();
        for (Object[] param : params) {
            testInetAddressFilterInvalidCidrFilter((String) param[0], (String) param[1]);
        }
    }

    public void testInetAddressFilterInvalidCidrFilter(final String filterString, final String checkAddr)
            throws Exception {
        final InetAddressFilter filter = getFilter();
        final MockFilterConfig config = new MockFilterConfig(servletContext);
        config.addInitParameter(InetAddressFilter.INET_ADDRESS_FILTERS, filterString);
        filter.init(config);

        final MockHttpServletRequest request = createMockHttpServletRequest(checkAddr);
        final MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, new MockFilterChain());
        Assert.assertTrue(response.getStatus() == HttpServletResponse.SC_FORBIDDEN);

        filter.destroy();
    }

    public Object[][] invalidCidrFilterParams() {
        return new Object[][] { { CIDR_ADDRESS_FILTER, "10.10.2.44" }, { CIDR_ADDRESS_FILTER, "10.10.1.221" },
                { CIDR_ADDRESS_FILTER, "10.10.1.150" }, { CIDR_ADDRESS_FILTER, "164.50.99.251" },
                { CIDR_ADDRESS_FILTER, "165.37.2.59" }, { CIDR_ADDRESS_FILTER, "164.50.2.59" },
                { CIDR_ADDRESS_FILTER, "101.0.21.2" } };
    }

    @Test(expected=ServletException.class )
    public void testInetAddressFilterInvalidAddressFilter() throws Exception {
        Object[][] params = invalidFilterParams();
        for (Object[] param : params) {
            testInetAddressFilterInvalidAddressFilter((String) param[0]);
        }
    }

    private void testInetAddressFilterInvalidAddressFilter(final String filterString) throws Exception {
        final InetAddressFilter filter = getFilter();
        final MockFilterConfig config = new MockFilterConfig(servletContext);
        config.addInitParameter(InetAddressFilter.INET_ADDRESS_FILTERS, filterString);
        filter.init(config);

        // final MockHttpServletRequest request =
        // createMockHttpServletRequest("10.10.10.10");
        // final MockHttpServletResponse response = new
        // MockHttpServletResponse();
        // filter.doFilter(request, response, new MockFilterChain());

        filter.destroy();
    }

    private Object[][] invalidFilterParams() {
        return new Object[][] { { "256.0.5.6" }, { "432.553.2344.211" }, { "4D3.234.FX3" }, { "6.70. * .322" },
                { "0.0.0.322" }, { "1-10.4.5.6" }, { "100.140-141.3.4" }, { "144.5.210-234.5" }, { "110.30.46.1-100" } };
    }

    @Test
    public void testInetAddressFilterStaticAddressFilter() throws Exception {
        Object[][] params = staticAddressInetAddressFilterParams();
        for (Object[] param : params) {
            testInetAddressFilterStaticAddressFilter((String) param[0], (String) param[1]);
        }
    }

    private Object[][] staticAddressInetAddressFilterParams() {
        return new Object[][] { { STATIC_ADDRESS_FILTER, "105.10.5.15" }, { STATIC_ADDRESS_FILTER, "206.123.45.90" },
                { STATIC_ADDRESS_FILTER, "184.210.209.99" }, { STATIC_ADDRESS_FILTER, "64.78.81.11" },
                { STATIC_ADDRESS_FILTER, "164.240.181.211" } };
    }

    private void testInetAddressFilterStaticAddressFilter(final String filterString, final String checkAddr)
            throws Exception {
        final InetAddressFilter filter = getFilter();
        final MockFilterConfig config = new MockFilterConfig(servletContext);
        config.addInitParameter(InetAddressFilter.INET_ADDRESS_FILTERS, filterString);
        filter.init(config);

        final MockHttpServletRequest request = createMockHttpServletRequest(checkAddr);
        final MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, new MockFilterChain());
        Assert.assertFalse(response.getStatus() == HttpServletResponse.SC_FORBIDDEN);

        filter.destroy();
    }

    @Test
    public void testInvalidInetAddressFilterStaticAddressFilter() throws Exception {
        Object[][] params = invalidStaticAddressInetAddressFilterParams();
        for (Object[] p : params) {
            testInvalidInetAddressFilterStaticAddressFilter((String) p[0], (String) p[1]);
        }
    }

    private Object[][] invalidStaticAddressInetAddressFilterParams() {
        return new Object[][] { { STATIC_ADDRESS_FILTER, "105.10.5.16" }, { STATIC_ADDRESS_FILTER, "206.124.45.90" },
                { STATIC_ADDRESS_FILTER, "182.210.209.99" }, { STATIC_ADDRESS_FILTER, "64.78.81.10" },
                { STATIC_ADDRESS_FILTER, "164.240.180.211" } };
    }

    private void testInvalidInetAddressFilterStaticAddressFilter(final String filterString, final String checkAddr)
            throws Exception {
        final InetAddressFilter filter = getFilter();
        final MockFilterConfig config = new MockFilterConfig(servletContext);
        config.addInitParameter(InetAddressFilter.INET_ADDRESS_FILTERS, filterString);
        filter.init(config);

        final MockHttpServletRequest request = createMockHttpServletRequest(checkAddr);
        final MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, new MockFilterChain());
        Assert.assertTrue(response.getStatus() == HttpServletResponse.SC_FORBIDDEN);

        filter.destroy();
    }

    @Test
    public void testInetAddressFilterComboAddressFilter() throws Exception {
        Object[][] params = comboAddressInetAddressFilterParams();
        for (Object[] p : params) {
            testInetAddressFilterComboAddressFilter((String) p[0], (String) p[1]);
        }
    }

    private void testInetAddressFilterComboAddressFilter(final String filterString, final String checkAddr)
            throws Exception {
        final InetAddressFilter filter = getFilter();
        final MockFilterConfig config = new MockFilterConfig(servletContext);
        config.addInitParameter(InetAddressFilter.INET_ADDRESS_FILTERS, filterString);
        filter.init(config);

        final MockHttpServletRequest request = createMockHttpServletRequest(checkAddr);
        final MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, new MockFilterChain());
        Assert.assertFalse(response.getStatus() == HttpServletResponse.SC_FORBIDDEN);

        filter.destroy();
    }

    private Object[][] comboAddressInetAddressFilterParams() {
        return new Object[][] { { COMBO_ADDRESS_FILTER, "105.11.5.15" }, { COMBO_ADDRESS_FILTER, "105.100.5.15" },
                { COMBO_ADDRESS_FILTER, "105.32.5.15" }, { COMBO_ADDRESS_FILTER, "206.123.6.90" },
                { COMBO_ADDRESS_FILTER, "206.123.0.90" }, { COMBO_ADDRESS_FILTER, "184.40.209.199" },
                { COMBO_ADDRESS_FILTER, "184.239.209.210" }, { COMBO_ADDRESS_FILTER, "64.78.81.199" },
                { COMBO_ADDRESS_FILTER, "5.78.81.254" }, { COMBO_ADDRESS_FILTER, "10.200.50.126" },
                { COMBO_ADDRESS_FILTER, "120.100.69.12" }, { COMBO_ADDRESS_FILTER, "11.235.50.210" },
                { COMBO_ADDRESS_FILTER, "15.240.181.139" }, { COMBO_ADDRESS_FILTER, "10.200.64.178" } };
    }

    @Test
    public void testInvalidInetAddressFilterComboAddressFilter() throws Exception {
        Object[][] params = invalidComboAddressInetAddressFilterParams();
        for (Object[] p : params) {
            testInvalidInetAddressFilterComboAddressFilter((String) p[0], (String) p[1]);
        }
    }

    private void testInvalidInetAddressFilterComboAddressFilter(final String filterString, final String checkAddr)
            throws Exception {
        final InetAddressFilter filter = getFilter();
        final MockFilterConfig config = new MockFilterConfig(servletContext);
        config.addInitParameter(InetAddressFilter.INET_ADDRESS_FILTERS, filterString);
        filter.init(config);

        final MockHttpServletRequest request = createMockHttpServletRequest(checkAddr);
        final MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, new MockFilterChain());
        Assert.assertTrue(response.getStatus() == HttpServletResponse.SC_FORBIDDEN);

        filter.destroy();
    }

    private Object[][] invalidComboAddressInetAddressFilterParams() {
        return new Object[][] { { COMBO_ADDRESS_FILTER, "184.1.209.98" }, { COMBO_ADDRESS_FILTER, "206.123.0.91" },
                { COMBO_ADDRESS_FILTER, "105.101.5.15" }, { COMBO_ADDRESS_FILTER, "105.101.5.16" },
                { COMBO_ADDRESS_FILTER, "65.78.81.2" }, { COMBO_ADDRESS_FILTER, "164.6.190.4" },
                { COMBO_ADDRESS_FILTER, "184.5.209.211" }, { COMBO_ADDRESS_FILTER, "201.123.5.90" },
                { COMBO_ADDRESS_FILTER, "11.7.49.7" }, { COMBO_ADDRESS_FILTER, "10.4.5.6" },
                { COMBO_ADDRESS_FILTER, "105.9.5.15" }, { COMBO_ADDRESS_FILTER, "10.155.50.1" },
                { COMBO_ADDRESS_FILTER, "15.240.181.212" }, { COMBO_ADDRESS_FILTER, "11.222.184.210" },
                { COMBO_ADDRESS_FILTER, "16.232.143.202" } };
    }

    @Test
    public void testInetAddressFilterDefaultMulticastAddressFilter() throws Exception {
        Object[][] params = multicastAddressInetAddressFilterParams();
        for (Object[] p : params) {
            testInetAddressFilterDefaultMulticastAddressFilter((String) p[0], (String) p[1]);
        }
    }
    
    private void testInetAddressFilterDefaultMulticastAddressFilter(final String filterString, final String checkAddr)
            throws Exception {
        final InetAddressFilter filter = getFilter();
        final MockFilterConfig config = new MockFilterConfig(servletContext);
        config.addInitParameter(InetAddressFilter.INET_ADDRESS_FILTERS, filterString);
        config.addInitParameter(InetAddressFilter.INET_ADDRESS_ALLOW_MULICAST, "false");
        filter.init(config);

        final MockHttpServletRequest request = createMockHttpServletRequest(checkAddr);
        final MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, new MockFilterChain());
        Assert.assertEquals("The " + filterString
                + " filter did not return the expected response when " + checkAddr + " was checked.", response.getStatus(), HttpServletResponse.SC_FORBIDDEN);

        filter.destroy();
    }
    
    private Object[][] multicastAddressInetAddressFilterParams() {
        return new Object[][] { { MULTICAST_ADDRESS_FILTER, "224.0.10.1" },
                { MULTICAST_ADDRESS_FILTER, "239.254.254.254" } };
    }    

    @Test
    public void testInetAddressFilterAllowMulticastInvalidAddressFilter() throws Exception {
        Object[][] params = invalidMulticastAddressInetAddressFilterParams();
        for (Object[] p : params) {
            testInetAddressFilterAllowMulticastInvalidAddressFilter((String) p[0], (String) p[1]);
        }
    }
    
    private void testInetAddressFilterAllowMulticastInvalidAddressFilter(final String filterString,
            final String checkAddr) throws Exception {
        final InetAddressFilter filter = getFilter();
        final MockFilterConfig config = new MockFilterConfig(servletContext);
        config.addInitParameter(InetAddressFilter.INET_ADDRESS_FILTERS, filterString);
        filter.init(config);

        final MockHttpServletRequest request = createMockHttpServletRequest(checkAddr);
        final MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, new MockFilterChain());
        Assert.assertTrue(response.getStatus() == HttpServletResponse.SC_FORBIDDEN);

        filter.destroy();
    }
    
    private Object[][] invalidMulticastAddressInetAddressFilterParams() {
        return new Object[][] { { MULTICAST_ADDRESS_FILTER, "224.0.10.2" },
                { MULTICAST_ADDRESS_FILTER, "239.254.254.250" } };
    }

    @Test
    public void testInetAddressFilterAllowMulticastAddressFilter() throws Exception {
        
        Object[][] params = multicastAddressInetAddressFilterParams();
        for (Object[] p : params) {
            testInetAddressFilterAllowMulticastAddressFilter((String) p[0], (String) p[1]);
        }
    }
    
    private  void testInetAddressFilterAllowMulticastAddressFilter(final String filterString, final String checkAddr)
            throws Exception {
        final InetAddressFilter filter = getFilter();
        final MockFilterConfig config = new MockFilterConfig(servletContext);
        config.addInitParameter(InetAddressFilter.INET_ADDRESS_FILTERS, filterString);
        config.addInitParameter(InetAddressFilter.INET_ADDRESS_ALLOW_MULICAST, "true");
        filter.init(config);

        final MockHttpServletRequest request = createMockHttpServletRequest(checkAddr);
        final MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, new MockFilterChain());
        Assert.assertFalse(response.getStatus() == HttpServletResponse.SC_FORBIDDEN);

        filter.destroy();
    }

    private MockHttpServletRequest createMockHttpServletRequest(final String remoteAddr) {
        // assign random methods and uri's as we're scrutinizing the remote
        // address
        final String method = HTTP_METHODS[Math.abs((int) System.currentTimeMillis()) % HTTP_METHODS.length];
        final String uri = URIS[Math.abs((int) System.currentTimeMillis()) % URIS.length];

        final MockHttpServletRequest request = new MockHttpServletRequest(servletContext, method, uri);
        request.setRemoteAddr(remoteAddr);
        return request;
    }

    private InetAddressFilter getFilter() {
        return (InetAddressFilter) applicationContext.getBean(Constants.INET_ADDRESS_FILTER);
    }
}