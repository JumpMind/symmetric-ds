/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>,
 *               Chris Henson <chenson42@users.sourceforge.net>
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

public class Constants {

    /**
     * This is a configuration id that can be used to apply parameters to all
     * configurations.  Each configuration can override global settings.
     */
    public static final String GLOBAL_CONFIGURATION_ID = "GLOBAL";
    
    public static final String PROPERTIES = "properties";
    
    public static final String CHANNEL_CONFIG = "config";
    
    public static final String CHANNEL_RELOAD = "reload";
    
    public static final String DATA_SOURCE = "dataSource";
    
    public static final String BOOTSTRAP_SERVICE = "bootstrapService";

    public static final String NODE_SERVICE = "nodeService";
    
    public static final String DATALOADER_SERVICE = "dataLoaderService";
    
    public static final String PARAMETER_SERVICE = "parameterService";
    
    public static final String DATALOADER = "dataLoader";
    
    public static final String INCOMING_BATCH_SERVICE = "incomingBatchService";
    
    public static final String DATAEXTRACTOR_SERVICE = "dataExtractorService";
    
    public static final String CONFIG_SERVICE = "configurationService";
    
    public static final String TRANSPORT_MANAGER = "transportManager";
    
    public static final String ACKNOWLEDGE_SERVICE = "acknowledgeService";
    
    public static final String REGISTRATION_SERVICE = "registrationService";
    
    public static final String PUSH_SERVICE = "pushService";
    
    public static final String PULL_SERVICE = "pullService";
    
    public static final String RUNTIME_CONFIG = "runtimeConfiguration";
    
    public static final String DB_DIALECT = "dbDialect";
    
    public static final String PUSH_JOB_TIMER = "pushJobTimer";
    
    public static final String PULL_JOB_TIMER = "pullJobTimer";
    
    public static final String PURGE_JOB_TIMER = "purgeJobTimer";
    
    public static final String HEARTBEAT_JOB_TIMER = "heartbeatJobTimer";
    
    public static final String SYNC_TRIGGERS_JOB_TIMER = "syncTriggersJobTimer";
    
    public static final String DATA_EXTRACTOR = "dataExtractor";
    
    public static final String OUTGOING_BATCH_SERVICE = "outgoingBatchService";
    
    public static final String PURGE_SERVICE = "purgeService";
    
    public static final String JDBC = "jdbcTemplate";
    
    public static final String DOWNLOAD_RATE = "downloadRateKb";
    
    public static final String MAX_CONCURRENT_WORKERS = "maxConcurrentWorkers";
    
}
