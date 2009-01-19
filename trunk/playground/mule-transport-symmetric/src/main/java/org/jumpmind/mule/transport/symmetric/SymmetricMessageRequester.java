/*
 * $Id: RetrieveMessageRequester.java 10961 2008-02-22 19:01:02Z dfeist $
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSource, Inc.  All rights reserved.  http://www.mulesource.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.jumpmind.mule.transport.symmetric;

import org.mule.api.MuleEvent;
import org.mule.api.MuleMessage;
import org.mule.api.endpoint.InboundEndpoint;
import org.mule.transport.AbstractMessageRequester;

/**
 * This dispatcher can only be used to receive message (as opposed to listening for them).
 * Trying to send or dispatch will throw an UnsupportedOperationException.
 *
 */

public class SymmetricMessageRequester extends AbstractMessageRequester
{
    public SymmetricMessageRequester(InboundEndpoint endpoint)
    {
        super(endpoint);
    }

    private SymmetricConnector castConnector()
    {
        return (SymmetricConnector) getConnector();
    }

    protected void doConnect() throws Exception
    {
    }

    protected void doDisconnect() throws Exception
    {
    }

    /**
     * @param event
     * @throws UnsupportedOperationException
     */
    protected void doDispatch(MuleEvent event) throws Exception
    {
        throw new UnsupportedOperationException("Cannot dispatch from a Pop3 connection");
    }

    /**
     * @param event
     * @return
     * @throws UnsupportedOperationException
     */
    protected MuleMessage doSend(MuleEvent event) throws Exception
    {
        throw new UnsupportedOperationException("Cannot send from a Pop3 connection");
    }

    protected MuleMessage doRequest(long timeout) throws Exception
    {
        return null;
    }

    protected void doDispose()
    {
    }

}