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

package org.jumpmind.symmetric.common;

public class PropertiesConstants {

    public final static String START_PULL_JOB = "symmetric.runtime.start.pull.job";
    public final static String START_PUSH_JOB = "symmetric.runtime.start.push.job";
    public final static String START_PURGE_JOB = "symmetric.runtime.start.purge.job";
    public final static String START_HEARTBEAT_JOB = "symmetric.runtime.start.heartbeat.job";
    public final static String START_SYNCTRIGGERS_JOB = "symmetric.runtime.start.synctriggers.job";
    
    
    public final static String DBPOOL_URL = "db.url";
    public final static String DBPOOL_DRIVER ="db.driver";
    public final static String DBPOOL_USER = "db.user";
    public final static String DBPOOL_PASSWORD = "db.password";
    public final static String DBPOOL_INITIAL_SIZE = "db.pool.initial.size";
    
    public final static String RUNTIME_CONFIG_TABLE_PREFIX = "sync.table.prefix";
}