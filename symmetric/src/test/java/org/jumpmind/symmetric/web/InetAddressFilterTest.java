/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Keith Naas <knaas@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.web;

import java.lang.reflect.Method;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.common.Constants;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockFilterConfig;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockServletContext;
import org.springframework.web.context.ConfigurableWebApplicationContext;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.XmlWebApplicationContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * This simply makes sure the SymmetricFilter is setup correctly.
 */
public class InetAddressFilterTest
{
    public static final String[] HTTP_METHODS = {"GET", "POST", "DELETE", "PUT", "TRACE", "OPTIONS"};

    public static final String[] URIS = {"/ack", "/push", "/pull"};

    public static final String COMBO_ADDRESS_FILTER = "105.100-10.5.15, 206.123.*.90, 184.*.209.210-99, 64-1.78.81.*, 164-120.*.181-50.*, 15-10.240-155.181-50.211-2";

    // public static final String COMBO_ADDRESS_FILTER = "105.100/10.5.15, 206.123.*.90, 184.*.209.210/99, 64/1.78.81.*,
    // 164/120.*.181/50.*, 15/10.240/155.181/50.211/2";

    public static final String STATIC_ADDRESS_FILTER = "105.10.5.15, 206.123.45.90, 184.210.209.99, 64.78.81.11, 164.240.181.211";

    public static final String MULTICAST_ADDRESS_FILTER = "224.0.10.1, 239.254.254.254";

    public static final String CIDR_ADDRESS_FILTER = "10.10.1.32/27, 164.37.0.0/16";

    protected ConfigurableWebApplicationContext applicationContext;

    protected ServletContext servletContext;

    @BeforeClass(alwaysRun = true)
    protected void springTestContextBeforeTestMethod(final Method method) throws Exception
    {
        servletContext = new MockServletContext();
        applicationContext = new XmlWebApplicationContext();
        applicationContext.setParent(new ClassPathXmlApplicationContext("classpath:symmetric.xml"));
        applicationContext.setServletContext(servletContext);
        applicationContext.setConfigLocations(new String[0]);
        applicationContext.refresh();

        servletContext.setAttribute(WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE, applicationContext);
    }

    @DataProvider(name = "staticAddressFilterParams")
    public Object[][] staticAddressInetAddressFilterParams()
    {
        return new Object[][] { {STATIC_ADDRESS_FILTER, "105.10.5.15"}, {STATIC_ADDRESS_FILTER, "206.123.45.90"},
            {STATIC_ADDRESS_FILTER, "184.210.209.99"}, {STATIC_ADDRESS_FILTER, "64.78.81.11"},
            {STATIC_ADDRESS_FILTER, "164.240.181.211"}};
    }

    @DataProvider(name = "invalidStaticAddressFilterParams")
    public Object[][] invalidStaticAddressInetAddressFilterParams()
    {
        return new Object[][] { {STATIC_ADDRESS_FILTER, "105.10.5.16"}, {STATIC_ADDRESS_FILTER, "206.124.45.90"},
            {STATIC_ADDRESS_FILTER, "182.210.209.99"}, {STATIC_ADDRESS_FILTER, "64.78.81.10"},
            {STATIC_ADDRESS_FILTER, "164.240.180.211"}};
    }

    @DataProvider(name = "comboAddressFilterParams")
    public Object[][] comboAddressInetAddressFilterParams()
    {
        return new Object[][] { {COMBO_ADDRESS_FILTER, "105.11.5.15"}, {COMBO_ADDRESS_FILTER, "105.100.5.15"},
            {COMBO_ADDRESS_FILTER, "105.32.5.15"}, {COMBO_ADDRESS_FILTER, "206.123.6.90"},
            {COMBO_ADDRESS_FILTER, "206.123.0.90"}, {COMBO_ADDRESS_FILTER, "184.40.209.199"},
            {COMBO_ADDRESS_FILTER, "184.239.209.210"}, {COMBO_ADDRESS_FILTER, "64.78.81.199"},
            {COMBO_ADDRESS_FILTER, "5.78.81.254"}, {COMBO_ADDRESS_FILTER, "10.200.50.126"},
            {COMBO_ADDRESS_FILTER, "120.100.69.12"}, {COMBO_ADDRESS_FILTER, "11.235.50.210"},
            {COMBO_ADDRESS_FILTER, "15.240.181.139"}, {COMBO_ADDRESS_FILTER, "10.200.64.178"}};
    }

    @DataProvider(name = "invalidComboAddressFilterParams")
    public Object[][] invalidComboAddressInetAddressFilterParams()
    {
        return new Object[][] { {COMBO_ADDRESS_FILTER, "184.1.209.98"}, {COMBO_ADDRESS_FILTER, "206.123.0.91"},
            {COMBO_ADDRESS_FILTER, "105.101.5.15"}, {COMBO_ADDRESS_FILTER, "105.101.5.16"},
            {COMBO_ADDRESS_FILTER, "65.78.81.2"}, {COMBO_ADDRESS_FILTER, "164.6.190.4"},
            {COMBO_ADDRESS_FILTER, "184.5.209.211"}, {COMBO_ADDRESS_FILTER, "201.123.5.90"},
            {COMBO_ADDRESS_FILTER, "11.7.49.7"}, {COMBO_ADDRESS_FILTER, "10.4.5.6"},
            {COMBO_ADDRESS_FILTER, "105.9.5.15"}, {COMBO_ADDRESS_FILTER, "10.155.50.1"},
            {COMBO_ADDRESS_FILTER, "15.240.181.212"}, {COMBO_ADDRESS_FILTER, "11.222.184.210"},
            {COMBO_ADDRESS_FILTER, "16.232.143.202"}};
    }

    @DataProvider(name = "cidrFilterParams")
    public Object[][] cidrFilterParams()
    {
        return new Object[][] { {CIDR_ADDRESS_FILTER, "10.10.1.44"}, {CIDR_ADDRESS_FILTER, "10.10.1.47"},
            {CIDR_ADDRESS_FILTER, "10.10.1.63"}, {CIDR_ADDRESS_FILTER, "164.37.99.251"},
            {CIDR_ADDRESS_FILTER, "164.37.2.59"}};
    }

    @DataProvider(name = "invalidCidrFilterParams")
    public Object[][] invalidCidrFilterParams()
    {
        return new Object[][] { {CIDR_ADDRESS_FILTER, "10.10.2.44"}, {CIDR_ADDRESS_FILTER, "10.10.1.221"},
            {CIDR_ADDRESS_FILTER, "10.10.1.150"}, {CIDR_ADDRESS_FILTER, "164.50.99.251"},
            {CIDR_ADDRESS_FILTER, "165.37.2.59"}, {CIDR_ADDRESS_FILTER, "164.50.2.59"},
            {CIDR_ADDRESS_FILTER, "101.0.21.2"}};
    }

    @DataProvider(name = "invalidFilterParams")
    public Object[][] invalidFilterParams()
    {
        return new Object[][] { {"256.0.5.6"}, {"432.553.2344.211"}, {"4D3.234.FX3"}, {"6.70. * .322"}, {"0.0.0.322"},
            {"1-10.4.5.6"}, {"100.140-141.3.4"}, {"144.5.210-234.5"}, {"110.30.46.1-100"}};
    }

    @DataProvider(name = "multicastAddressFilterParams")
    public Object[][] multicastAddressInetAddressFilterParams()
    {
        return new Object[][] { {MULTICAST_ADDRESS_FILTER, "224.0.10.1"}, {MULTICAST_ADDRESS_FILTER, "239.254.254.254"}};
    }

    @DataProvider(name = "invalidMulticastAddressFilterParams")
    public Object[][] invalidMulticastAddressInetAddressFilterParams()
    {
        return new Object[][] { {MULTICAST_ADDRESS_FILTER, "224.0.10.2"}, {MULTICAST_ADDRESS_FILTER, "239.254.254.250"}};
    }

    @DataProvider(name = "parameterServiceCheckAddrParams")
    public Object[][] parameterServiceCheckAddrParams()
    {
        return new Object[][] { {"10.4.201.179"}, {"10.1.200.4"}, {"10.4.100.43"}, {"11.100.121.83"},
            {"11.100.148.221"}, {"11.100.100.9"}, {"100.50.253.172"}, {"100.50.0.3"}, {"100.50.94.254"}};
    }

    @DataProvider(name = "invalidParameterServiceCheckAddrParams")
    public Object[][] invalidParameterServiceCheckAddrParams()
    {
        return new Object[][] { {"12.4.201.179"}, {"10.7.200.4"}, {"10.4.0.43"}, {"6.100.121.83"}, {"11.99.148.221"},
            {"101.100.121.83"}, {"11.100.201.83"}, {"100.49.121.83"}, {"6.4.3.9"}};
    }

    @Test(groups = "continuous", dataProvider = "parameterServiceCheckAddrParams")
    public void testInetAddressFilterParameterServiceFilter(final String checkAddr) throws Exception
    {
        final InetAddressFilter filter = getFilter();
        final MockFilterConfig config = new MockFilterConfig(servletContext);
        filter.init(config);

        final MockHttpServletRequest request = createMockHttpServletRequest(checkAddr);
        final MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, new MockFilterChain());
        Assert.assertFalse(response.getStatus() == HttpServletResponse.SC_FORBIDDEN);

        filter.destroy();
    }

    @Test(groups = "continuous", dataProvider = "cidrFilterParams")
    public void testInetAddressFilterCidrFilter(final String filterString, final String checkAddr) throws Exception
    {
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

    @Test(groups = "continuous", dataProvider = "invalidCidrFilterParams")
    public void testInetAddressFilterInvalidCidrFilter(final String filterString, final String checkAddr)
        throws Exception
    {
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

    @Test(groups = "continuous", dataProvider = "invalidFilterParams", expectedExceptions = {
        IllegalArgumentException.class, ServletException.class})
    public void testInetAddressFilterInvalidAddressFilter(final String filterString) throws Exception
    {
        final InetAddressFilter filter = getFilter();
        final MockFilterConfig config = new MockFilterConfig(servletContext);
        config.addInitParameter(InetAddressFilter.INET_ADDRESS_FILTERS, filterString);
        filter.init(config);

        // final MockHttpServletRequest request = createMockHttpServletRequest("10.10.10.10");
        // final MockHttpServletResponse response = new MockHttpServletResponse();
        // filter.doFilter(request, response, new MockFilterChain());

        filter.destroy();
    }

    @Test(groups = "continuous", dataProvider = "staticAddressFilterParams")
    public void testInetAddressFilterStaticAddressFilter(final String filterString, final String checkAddr)
        throws Exception
    {
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

    @Test(groups = "continuous", dataProvider = "invalidStaticAddressFilterParams")
    public void testInvalidInetAddressFilterStaticAddressFilter(final String filterString, final String checkAddr)
        throws Exception
    {
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

    @Test(groups = "continuous", dataProvider = "comboAddressFilterParams")
    public void testInetAddressFilterComboAddressFilter(final String filterString, final String checkAddr)
        throws Exception
    {
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

    @Test(groups = "continuous", dataProvider = "invalidComboAddressFilterParams")
    public void testInvalidInetAddressFilterComboAddressFilter(final String filterString, final String checkAddr)
        throws Exception
    {
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

    @Test(groups = "continuous", dataProvider = "multicastAddressFilterParams")
    public void testInetAddressFilterDefaultMulticastAddressFilter(final String filterString, final String checkAddr)
        throws Exception
    {

        // final InetAddressFilter filter = new InetAddressFilter();
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

    @Test(groups = "continuous", dataProvider = "invalidMulticastAddressFilterParams")
    public void testInetAddressFilterAllowMulticastInvalidAddressFilter(final String filterString,
        final String checkAddr) throws Exception
    {
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

    @Test(groups = "continuous", dataProvider = "multicastAddressFilterParams")
    public void testInetAddressFilterAllowMulticastAddressFilter(final String filterString, final String checkAddr)
        throws Exception
    {
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

    private MockHttpServletRequest createMockHttpServletRequest(final String remoteAddr)
    {
        // assign random methods and uri's as we're scrutinizing the remote address
        final String method = HTTP_METHODS[Math.abs((int) System.currentTimeMillis()) % HTTP_METHODS.length];
        final String uri = URIS[Math.abs((int) System.currentTimeMillis()) % URIS.length];

        final MockHttpServletRequest request = new MockHttpServletRequest(servletContext, method, uri);
        request.setRemoteAddr(remoteAddr);
        return request;
    }

    private InetAddressFilter getFilter()
    {
        return (InetAddressFilter) applicationContext.getBean(Constants.INET_ADDRESS_FILTER);
    }
}
