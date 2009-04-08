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

package org.jumpmind.symmetric.test;

public class TestConstants {
    public final static String TEST_PREFIX = "sym_";
    public final static String TEST_CLIENT_EXTERNAL_ID = "00001";
    public final static String TEST_ROOT_EXTERNAL_ID = "00000";
    public static final String TEST_ROOT_NODE_GROUP = "test-root-group";
    public static final String TEST_CLIENT_NODE_GROUP = "test-node-group";
    public static final String TEST_DROP_ALL_SCRIPT = "/test-data-drop-all.sql";
    public static final String TEST_DROP_SEQ_SCRIPT = "/test-data-drop-";
    public static final String TEST_ROOT_DOMAIN_SETUP_SCRIPT = "-integration-root-setup.sql";
    public static final String TEST_CONTINUOUS_SETUP_SCRIPT = "-database-setup.sql";
    public static final String TEST_CONTINUOUS_NODE_GROUP = "test-root-group";
    public static final String TEST_CHANNEL_ID = "testchannel";
    public static final int TEST_AUDIT_ID = 1;

    public static final String MYSQL = "mysql";
}
