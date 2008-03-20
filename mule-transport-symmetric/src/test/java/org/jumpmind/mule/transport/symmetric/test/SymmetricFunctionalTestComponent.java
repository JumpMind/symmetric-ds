/*
 * $Id: JdbcFunctionalTestComponent.java 10787 2008-02-12 18:51:50Z dfeist $
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSource, Inc.  All rights reserved.  http://www.mulesource.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.jumpmind.mule.transport.symmetric.test;

import org.jumpmind.symmetric.model.Data;
import org.mule.api.MuleEventContext;
import org.mule.tck.functional.FunctionalTestComponent;

public class SymmetricFunctionalTestComponent extends FunctionalTestComponent
{
    public Object onCall(MuleEventContext context) throws Exception
    {
        super.onCall(context);

        Data data = (Data)context.getMessage().getPayload();
        return data + " Received";
    }
}
