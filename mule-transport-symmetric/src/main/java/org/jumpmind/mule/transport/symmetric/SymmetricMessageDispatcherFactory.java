/*
 * $Id: MessageDispatcherFactory.vm 11079 2008-02-27 15:52:01 +0000 (Wed, 27 Feb 2008) tcarlson $
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSource, Inc.  All rights reserved.  http://www.mulesource.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.jumpmind.mule.transport.symmetric;

import org.mule.api.MuleException;
import org.mule.api.endpoint.ImmutableEndpoint;
import org.mule.api.transport.MessageDispatcher;
import org.mule.transport.AbstractMessageDispatcherFactory;

/**
 * <code>SymmetricMessageDispatcherFactory</code> Todo document
 */

public class SymmetricMessageDispatcherFactory extends AbstractMessageDispatcherFactory
{

    /* For general guidelines on writing transports see
       http://mule.mulesource.org/display/MULE/Writing+Transports */

    public MessageDispatcher create(ImmutableEndpoint endpoint) throws MuleException
    {
        return new SymmetricMessageDispatcher(endpoint);
    }

}
