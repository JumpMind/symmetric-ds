/*
 * $Id: MessageReceiverTestCase.vm 11079 2008-02-27 15:52:01 +0000 (Wed, 27 Feb 2008) tcarlson $
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSource, Inc.  All rights reserved.  http://www.mulesource.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.jumpmind.mule.transport.symmetric;

import org.mule.api.endpoint.EndpointBuilder;
import org.mule.api.endpoint.InboundEndpoint;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.api.service.Service;
import org.mule.api.transport.MessageReceiver;
import org.mule.config.i18n.MessageFactory;
import org.mule.endpoint.EndpointURIEndpointBuilder;
import org.mule.transport.AbstractMessageReceiverTestCase;

import com.mockobjects.dynamic.Mock;

public class SymmetricMessageReceiverTestCase extends
		AbstractMessageReceiverTestCase {

    private SymmetricConnector connector;

    protected void doSetUp() throws Exception
    {
        connector = new SymmetricConnector();
        connector.setName("TestConnector");

        connector.setMuleContext(muleContext);
        muleContext.getRegistry().registerConnector(connector);
        
        super.doSetUp();
    }

	public MessageReceiver getMessageReceiver() throws Exception {
		Mock mockService = new Mock(Service.class);
		mockService.expectAndReturn("getResponseRouter", null);
		return new SymmetricMessageReceiver(endpoint.getConnector(),
				(Service) mockService.proxy(), endpoint);
	}

	public InboundEndpoint getEndpoint() throws Exception {
	    EndpointBuilder builder = new EndpointURIEndpointBuilder("symmetric://test", muleContext);
        if (connector == null)
        {
            throw new InitialisationException(MessageFactory.createStaticMessage("Connector has not been initialized."), null);
        }
        builder.setConnector(connector);
        endpoint = muleContext.getRegistry().lookupEndpointFactory().getInboundEndpoint(builder);
        return endpoint;
	}

}
