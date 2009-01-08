/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Eric Long <erilong@users.sourceforge.net>,
 *               Keith Naas <knaas@users.sourceforge.net>
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

package org.jumpmind.symmetric.web;

public class WebConstants {

    public static final int REGISTRATION_NOT_OPEN = 656;

    public static final int REGISTRATION_REQUIRED = 657;

    public static final String ACK_BATCH_NAME = "batch-";

    public static final String ACK_BATCH_OK = "ok";

    public static final String ACK_NODE_ID = "nodeId-";

    public static final String ACK_NETWORK_MILLIS = "network-";

    public static final String ACK_FILTER_MILLIS = "filter-";

    public static final String ACK_DATABASE_MILLIS = "database-";

    public static final String ACK_BYTE_COUNT = "byteCount-";

    public static final String ACK_SQL_STATE = "sqlState-";

    public static final String ACK_SQL_CODE = "sqlCode-";

    public static final String ACK_SQL_MESSAGE = "sqlMessage-";

    public static final String NODE_ID = "nodeId";

    public static final String NODE_GROUP_ID = "nodeGroupId";

    public static final String EXTERNAL_ID = "externalId";

    public static final String SYMMETRIC_VERSION = "symmetricVersion";

    public static final String SYNC_URL = "syncURL";

    public static final String SCHEMA_VERSION = "schemaVersion";

    public static final String DATABASE_TYPE = "databaseType";

    public static final String DATABASE_VERSION = "databaseVersion";

    public static final String SECURITY_TOKEN = "securityToken";

}
