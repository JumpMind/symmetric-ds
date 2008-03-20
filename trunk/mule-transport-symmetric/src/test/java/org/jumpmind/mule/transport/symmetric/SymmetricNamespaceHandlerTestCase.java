/*
 * $Id: NamespaceHandlerTestCase.vm 10621 2008-01-30 12:15:16 +0000 (Wed, 30 Jan 2008) dirk.olmes $
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSource, Inc.  All rights reserved.  http://www.mulesource.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.jumpmind.mule.transport.symmetric;

import org.jumpmind.mule.transport.symmetric.SymmetricConnector;
import org.mule.tck.FunctionalTestCase;

/**
 * TODO
 */
public class SymmetricNamespaceHandlerTestCase extends FunctionalTestCase
{
    protected String getConfigResources()
    {
        //TODO You'll need to edit this file to configure the properties specific to your transport
        return "symmetric-namespace-config.xml";
    }

    public void testSymmetricConfig() throws Exception
    {
        SymmetricConnector c = (SymmetricConnector) muleContext.getRegistry().lookupConnector("symmetricConnector");
        assertNotNull(c);
        assertTrue(c.isConnected());
        assertTrue(c.isStarted());

        //TODO Assert specific properties are configured correctly


    }
}
