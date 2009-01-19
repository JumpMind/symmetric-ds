/*
 * $Id: ConnectorTestCase.vm 10621 2008-01-30 12:15:16 +0000 (Wed, 30 Jan 2008) dirk.olmes $
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSource, Inc.  All rights reserved.  http://www.mulesource.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.jumpmind.mule.transport.symmetric;

import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.mule.api.transport.Connector;
import org.mule.transport.AbstractConnectorTestCase;

public class SymmetricConnectorTestCase extends AbstractConnectorTestCase {

    public Connector createConnector() throws Exception {

        SymmetricConnector c = new SymmetricConnector();
        c.setName("Test");
        return c;
    }

    public String getTestEndpointURI() {
        return "symmetric://test?s=s";
    }

    public Object getValidMessage() throws Exception {
        return new Data("testTable", DataEventType.INSERT, "1,2,3", "1",
                new TriggerHistory());
    }

    public void testProperties() throws Exception {
        // TODO test setting and retrieving any custom properties on the
        // Connector as necessary
    }

    @Override
    public void testConnectorMessageAdapter() throws Exception
    {
        Connector connector = getConnector();
        assertNotNull(connector);
        //org.mule.api.transport.MessageAdapter adapter = connector.getMessageAdapter(getValidMessage());
    }

    @Override
    public void testConnectorMessageRequesterFactory() throws Exception
    {
        Connector connector = getConnector();
        assertNotNull(connector);
        //org.mule.api.transport.MessageRequesterFactory factory = connector.getRequesterFactory();
    }

    
    

}
