/*
 * $Id: MessageAdapter.vm 10621 2008-01-30 12:15:16 +0000 (Wed, 30 Jan 2008) dirk.olmes $
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSource, Inc.  All rights reserved.  http://www.mulesource.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.mule.transport.symmetric;

import org.mule.api.MessagingException;
import org.mule.transport.AbstractMessageAdapter;

/**
 * <code>SymmetricMessageAdapter</code> TODO document
 */
public class SymmetricMessageAdapter extends AbstractMessageAdapter
{
 
    /* For general guidelines on writing transports see
       http://mule.mulesource.org/display/MULE/Writing+Transports */

    /* IMPLEMENTATION NOTE: The MessageAdapter is used to wrap an underlying
       message. It should store a copy of the underlying message as an
       instance variable. */

    /* IMPLEMENTATION NOTE: If the underlying transport data is available as a stream
        it is recommended that you pass the stream object into the MessageAdapter as the payload.
        This will ensure that Mule will use streaming where possible. */
    
    public SymmetricMessageAdapter(Object message) throws MessagingException
    {
        /* IMPLEMENTATION NOTE: The constructor should determine that the
           message is of the correct type or throw an exception i.e.
        
        if (message instanceof byte[]) {
            this.message = (byte[]) message;
        } else {
            throw new MessageTypeNotSupportedException(message, getClass());
        }
        */
    }

    public String getPayloadAsString(String encoding) throws Exception
    {
        // TODO return the string representation of the wrapped message
        throw new UnsupportedOperationException("getPayloadAsString");
    }

    public byte[] getPayloadAsBytes() throws Exception
    {
        // TODO return the byte[] representation of the wrapped message
        throw new UnsupportedOperationException("getPayloadAsBytes");
    }

    public Object getPayload()
    {
        // TODO return the actual wrapped message
        throw new UnsupportedOperationException("getPayload");
    }

}
