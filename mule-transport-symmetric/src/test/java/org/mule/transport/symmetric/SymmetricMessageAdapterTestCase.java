/*
 * $Id: MessageAdapterTestCase.vm 10621 2008-01-30 12:15:16 +0000 (Wed, 30 Jan 2008) dirk.olmes $
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSource, Inc.  All rights reserved.  http://www.mulesource.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.mule.transport.symmetric;

import org.mule.api.MessagingException;
import org.mule.api.transport.MessageAdapter;
import org.mule.tck.providers.AbstractMessageAdapterTestCase;

public class SymmetricMessageAdapterTestCase extends AbstractMessageAdapterTestCase
{

    /* For general guidelines on writing transports see
       http://mule.mulesource.org/display/MULE/Writing+Transports */

    public Object getValidMessage() throws Exception
    {
        // TODO Create a valid message for your transport
        throw new UnsupportedOperationException("getValidMessage");
    }

    public MessageAdapter createAdapter(Object payload) throws MessagingException
    {
        return new SymmetricMessageAdapter(payload);
    }

}
