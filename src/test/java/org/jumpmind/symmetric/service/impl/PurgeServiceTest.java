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

package org.jumpmind.symmetric.service.impl;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.junit.Test;

public class PurgeServiceTest extends AbstractDatabaseTest {
        
    public PurgeServiceTest() throws Exception {
        super();
    }

    public PurgeServiceTest(String dbName) {
        super(dbName);
    }

    @Test
    public void testThatPurgeExecutes() {
        IPurgeService service = find(Constants.PURGE_SERVICE);
        service.purge();
    }
}
