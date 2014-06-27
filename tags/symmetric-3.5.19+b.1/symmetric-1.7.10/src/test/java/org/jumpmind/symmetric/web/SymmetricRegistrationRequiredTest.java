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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

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
public class SymmetricRegistrationRequiredTest extends AbstractSymmetricFilterTest {

    private static final String BAD_SECURITY_TOKEN = "2";
    private static final String BAD_NODE_ID = "2";

    public SymmetricRegistrationRequiredTest(String method, String uri, Map<String, String> parameters)
            throws Exception {
        super(method, uri, parameters);
    }

    @Parameters
    public static Collection<Object[]> authenticationFilterRegistrationRequiredParams() {
        final Map<String, String> badAuthentication = new HashMap<String, String>();
        badAuthentication.put(WebConstants.SECURITY_TOKEN, BAD_SECURITY_TOKEN);
        badAuthentication.put(WebConstants.NODE_ID, BAD_NODE_ID);
        return Arrays.asList(new Object[][] { { "GET", "/ack", badAuthentication },
                { "GET", "/pull", badAuthentication }, { "GET", "/push", badAuthentication } });
    }

    @Test
    public void testAuthenticationFilterRegistrationRequired() throws Exception {
        final SymmetricFilter filter = new SymmetricFilter();
        filter.init(new MockFilterConfig(servletContext));

        final MockHttpServletRequest request = TestSetupUtil.createMockHttpServletRequest(servletContext, method, uri,
                parameters);
        final MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, new MockFilterChain());
        Assert.assertEquals(response.getStatus(), WebConstants.REGISTRATION_REQUIRED);
        filter.destroy();
    }

}
