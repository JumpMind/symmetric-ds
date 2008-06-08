/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.config;

import org.jumpmind.symmetric.AbstractDatabaseTest;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RuntimeConfigTest extends AbstractDatabaseTest {

    @SuppressWarnings({ "unchecked", "deprecation" })
    @Test(groups="continuous")
    public void testRuntimeConfig() throws Exception {
        String actual = getParameterService().getString(ParameterConstants.START_RUNTIME_SCHEMA_VERSION);
        Assert.assertNotSame(actual, TestConfig.SCHEMA_VERSION);        
        getParameterService().saveParameter(ParameterConstants.RUNTIME_CONFIGURATION_CLASS, TestConfig.class.getName());
        actual = getParameterService().getString(ParameterConstants.START_RUNTIME_SCHEMA_VERSION);
        Assert.assertEquals(actual, TestConfig.SCHEMA_VERSION);
    }

}
