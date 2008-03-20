/*
 * $Id: JdbcFunctionalTestCase.java 10789 2008-02-12 20:04:43Z dfeist $
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSource, Inc.  All rights reserved.  http://www.mulesource.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.jumpmind.mule.transport.symmetric.functional;

import org.mule.api.MuleMessage;
import org.mule.module.client.MuleClient;

public class SymmetricFunctionalTestCase extends AbstractSymmetricFunctionalTestCase
{
    public static final String DEFAULT_MESSAGE = "Test Message";
    
    public SymmetricFunctionalTestCase()
    {
        setPopulateTestData(false);
    }
    
    protected String getConfigResources()
    {
        return super.getConfigResources() + ",symmetric-functional-config.xml";
    }

    public void testReceive() throws Exception
    {
        MuleClient client = new MuleClient();
        MuleMessage message = client.request("symmetric://test", 1000);
    }

}
