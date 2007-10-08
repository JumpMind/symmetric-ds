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

import org.testng.annotations.Test;

public class RuntimeConfigFactoryTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testRuntimeConfig() throws Exception {
        RuntimeConfigFactory r = new RuntimeConfigFactory();
        r.setDefaultInstance(new TestDefaultInstance());
        assert (r.isSingleton());
        assert (r.getObjectType().isAssignableFrom(IRuntimeConfig.class));
        assert (r.getObject() != null && r.getObject() instanceof IRuntimeConfig);
        r.setClassName("org.jumpmind.symmetric.PropertyRuntimeConfig");
        assert (r.getObject() != null && r.getObject() instanceof IRuntimeConfig);
    }

    class TestDefaultInstance implements IRuntimeConfig {

        public String getNodeGroupId() {
            return null;
        }

        public String getRegistrationUrl() {
            return null;
        }

        public String getExternalId() {
            return null;
        }

        public String getMyUrl() {
            return null;
        }

        public String getSchemaVersion() {
            return null;
        }

    }
}
