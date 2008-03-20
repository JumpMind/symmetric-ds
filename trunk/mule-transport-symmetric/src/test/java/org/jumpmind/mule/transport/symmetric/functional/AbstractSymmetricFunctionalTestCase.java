/*
 * $Id: AbstractJdbcFunctionalTestCase.java 10787 2008-02-12 18:51:50Z dfeist $
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSource, Inc.  All rights reserved.  http://www.mulesource.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.jumpmind.mule.transport.symmetric.functional;

import org.jumpmind.mule.transport.symmetric.SymmetricConnector;
import org.mule.tck.FunctionalTestCase;

public abstract class AbstractSymmetricFunctionalTestCase extends FunctionalTestCase
{
    protected static final String[] TEST_VALUES = {"Test", "The Moon", "Terra"};

    private boolean populateTestData = true;
    
    SymmetricConnector symmetricConnector = null;
    
    protected String getConfigResources()
    {
        return "symmetric-connector.xml";
    }

    // @Override
    protected void doSetUp() throws Exception
    {
        super.doSetUp();

        symmetricConnector = (SymmetricConnector) muleContext.getRegistry().lookupConnector("symmetricConnector");

    }


    
    public boolean isPopulateTestData()
    {
        return populateTestData;
    }

    public void setPopulateTestData(boolean populateTestData)
    {
        this.populateTestData = populateTestData;
    }
}


