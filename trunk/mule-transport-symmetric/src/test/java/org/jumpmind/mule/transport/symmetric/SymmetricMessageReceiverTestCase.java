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

import org.jumpmind.mule.transport.symmetric.SymmetricConnector;
import org.jumpmind.mule.transport.symmetric.SymmetricMessageReceiver;
import org.mule.api.endpoint.EndpointURI;
import org.mule.api.service.Service;
import org.mule.api.transport.MessageReceiver;
import org.mule.endpoint.InboundEndpoint;
import org.mule.endpoint.MuleEndpointURI;
import org.mule.tck.providers.AbstractMessageReceiverTestCase;

import com.mockobjects.dynamic.Mock;

public class SymmetricMessageReceiverTestCase extends
		AbstractMessageReceiverTestCase {

	/*
	 * For general guidelines on writing transports see
	 * http://mule.mulesource.org/display/MULE/Writing+Transports
	 */

	public MessageReceiver getMessageReceiver() throws Exception {
		Mock mockService = new Mock(Service.class);
		mockService.expectAndReturn("getResponseTransformer", null);
		return new SymmetricMessageReceiver(endpoint.getConnector(),
				(Service) mockService.proxy(), endpoint);
	}

	public InboundEndpoint getEndpoint() throws Exception {
		InboundEndpoint endpoint = new InboundEndpoint();
		endpoint.setEndpointURI(new MuleEndpointURI("symmetric:http://localhost:1234"));
		endpoint.setConnector(new SymmetricConnector());
		return endpoint;
	}

}
