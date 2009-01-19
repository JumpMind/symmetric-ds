/*
 * $Id: RetrieveMessageRequesterFactory.java 10961 2008-02-22 19:01:02Z dfeist $
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSource, Inc.  All rights reserved.  http://www.mulesource.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.jumpmind.mule.transport.symmetric;

import org.mule.api.MuleException;
import org.mule.api.endpoint.InboundEndpoint;
import org.mule.api.transport.MessageRequester;
import org.mule.transport.AbstractMessageRequesterFactory;

/**
 * A source of mail receiving message dispatchers.
 * The dispatcher can only be used to receive message (as apposed to
 * listening for them). Trying to send or dispatch will throw an
 * {@link UnsupportedOperationException}.
 */

public class SymmetricMessageRequesterFactory extends AbstractMessageRequesterFactory
{
    /**
     * By default client connections are closed after the request.
     */
    // @Override
    public boolean isCreateRequesterPerRequest()
    {
        return true;
    }

    public MessageRequester create(InboundEndpoint endpoint) throws MuleException
    {
        return new SymmetricMessageRequester(endpoint);
    }

}