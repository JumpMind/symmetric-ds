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
package org.jumpmind.symmetric.service;

public class ClusterConstants {

    public static final String COMMON_LOCK_ID = "common";
    
    public static final String ROUTE = "ROUTE";
    public static final String PUSH = "PUSH";
    public static final String PULL = "PULL";
    public static final String PURGE_OUTGOING = "PURGE_OUTGOING";
    public static final String PURGE_INCOMING = "PURGE_INCOMING";
    public static final String PURGE_STATISTICS = "PURGE_STATISTICS";
    public static final String PURGE_DATA_GAPS = "PURGE_DATA_GAPS";
    public static final String HEARTBEAT = "HEARTBEAT";
    public static final String SYNCTRIGGERS = "SYNCTRIGGERS";
    
}
