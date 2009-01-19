/*
 * \$Id: EndpointTestCase.vm 10621 2008-01-30 12:15:16 +0000 (Wed, 30 Jan 2008) dirk.olmes $
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSource, Inc.  All rights reserved.  http://www.mulesource.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.jumpmind.mule.transport.symmetric;

import org.mule.api.endpoint.EndpointURI;
import org.mule.endpoint.MuleEndpointURI;
import org.mule.tck.AbstractMuleTestCase;

public class SymmetricEndpointTestCase extends AbstractMuleTestCase
{

    public void testValidEndpointURI() throws Exception
    {
        EndpointURI url = new MuleEndpointURI("symmetric://test:1022");
        assertEquals("symmetric://test:1022", url.getUri().toASCIIString());
        assertEquals("symmetric", url.getScheme());
        assertNull(url.getEndpointName());
        assertEquals(1022, url.getPort());
        assertEquals("test", url.getHost());
        assertEquals(0, url.getParams().size());

        url = new MuleEndpointURI("symmetric://test:1022?x=y");
        assertEquals("symmetric://test:1022?x=y", url.getUri().toASCIIString());
        assertEquals("symmetric", url.getScheme());
        assertNull(url.getEndpointName());
        assertEquals(1022, url.getPort());
        assertEquals("test", url.getHost());
        assertEquals(1, url.getParams().size());
        // assertTrue(url.getParams().contains("x"));
        // assertEquals("y", url.getParams().get("x"));
    }

}
