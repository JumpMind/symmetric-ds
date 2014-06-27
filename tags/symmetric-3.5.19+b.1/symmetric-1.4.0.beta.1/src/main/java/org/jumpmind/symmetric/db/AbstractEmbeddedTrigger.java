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
package org.jumpmind.symmetric.db;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.hsqldb.types.Binary;
import org.jumpmind.symmetric.SymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;

/**
 * This class implements the functionality needed by (most) java-based symmetric
 * triggers.
 */
public abstract class AbstractEmbeddedTrigger {

    protected static final Log logger = LogFactory.getLog(AbstractEmbeddedTrigger.class);

    protected static final FastDateFormat dateFormatter = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.S");

    protected IDataService dataService;

    protected IConfigurationService configurationService;

    protected INodeService nodeService;

    protected IDbDialect dbDialect;

    protected Table table;

    protected TriggerHistory triggerHistory;

    protected Trigger trigger;

    protected DataEventType triggerType;

    protected String tableName;

    protected Set<String> excludedColumns;

    protected List<String> includedColumns;

    protected void initialize(DataEventType triggerType, String tableName) {
        this.triggerType = triggerType;
        this.tableName = tableName;
        SymmetricEngine engine = SymmetricEngine.findEngineByName(getEngineName().toLowerCase());
        this.dataService = getDataService(engine);
        this.configurationService = getConfigurationService(engine);
        this.nodeService = getNodeService(engine);
        this.dbDialect = getDbDialect(engine);
        this.triggerHistory = configurationService.getHistoryRecordFor(getTriggerHistId());
        this.trigger = configurationService.getTriggerById(triggerHistory.getTriggerId());
        this.table = dbDialect.getMetaDataFor(null, trigger.getSourceSchemaName(), tableName, true);
        initColumnNames(trigger);
    }

    protected abstract String getEngineName();

    protected abstract int getTriggerHistId();

    protected abstract String getTransactionId(Object[] oldRow, Object[] newRow);

    protected String formatRowData(Object[] oldRow, Object[] newRow) {
        if (triggerType == DataEventType.UPDATE || triggerType == DataEventType.INSERT) {
            return formatAsCsv(getOrderedColumnValues(newRow));
        } else {
            return null;
        }
    }

    protected String formatPkRowData(Object[] oldRow, Object[] newRow) {
        if (triggerType == DataEventType.UPDATE || triggerType == DataEventType.DELETE) {
            return formatAsCsv(getPrimaryKeys(oldRow));
        } else {
            return null;
        }
    }

    protected String formatAsCsv(Object[] data) {
        StringBuilder b = new StringBuilder();
        if (data != null) {
            for (Object object : data) {
                if (object != null) {
                    if (object instanceof String) {
                        b.append("\"");
                        b.append(StringUtils
                                .replace(StringUtils.replace(object.toString(), "\\", "\\\\"), "\"", "\\\""));
                        b.append("\"");
                    } else if (object instanceof Number) {
                        b.append("\"");
                        b.append(object);
                        b.append("\"");
                    } else if (object instanceof Date) {
                        b.append(dateFormatter.format((Date) object));
                    } else if (object instanceof byte[]) {
                        b.append("\"");
                        b.append(Base64.encodeBase64((byte[]) object));
                        b.append("\"");
                    } else if (object instanceof Binary) {
                        b.append("\"");
                        Binary d = (Binary) object;
                        b.append(new String(Base64.encodeBase64(d.getBytes())));
                        b.append("\"");
                    } else if (object instanceof Boolean) {
                        b.append(((Boolean) object) ? "\"1\"" : "\"0\"");
                    } else {
                        throw new IllegalStateException("Could not format " + object + " which is of type "
                                + object.getClass().getName());
                    }
                }
                b.append(",");
            }
            b.deleteCharAt(b.length() - 1);
        }
        return b.toString();
    }

    protected Data createData(Object[] oldRow, Object[] newRow) {
        Data data = new Data(StringUtils.isBlank(trigger.getTargetTableName()) ? tableName : trigger
                .getTargetTableName(), triggerType, formatRowData(oldRow, newRow), formatPkRowData(oldRow, newRow),
                triggerHistory);
        return data;
    }

    protected Object[] getPrimaryKeys(Object[] allValues) {
        Column[] keys = table.getPrimaryKeyColumns();
        if (keys == null) {
            keys = table.getColumns();
        }
        Object[] keyValues = new Object[keys.length];
        for (int i = 0; i < keys.length; i++) {
            keyValues[i] = allValues[table.getColumnIndex(keys[i])];
        }
        return keyValues;
    }

    protected Object[] getOrderedColumnValues(Object[] allValues) {
        Column[] columns = table.getColumns();
        Object[] values = new Object[columns.length - excludedColumns.size()];
        int x = 0;
        for (int i = 0; i < columns.length; i++) {
            if (!excludedColumns.contains(columns[i].getName().toLowerCase())) {
                values[x++] = allValues[i];
            }
        }
        return values;
    }

    private void initColumnNames(Trigger trigger) {
        excludedColumns = new HashSet<String>();
        String nameString = trigger.getExcludedColumnNames();
        if (!StringUtils.isBlank(nameString)) {
            String[] values = nameString.split(",");
            for (String string : values) {
                excludedColumns.add(string.toLowerCase());
            }
        }

        includedColumns = new ArrayList<String>();
        Column[] columns = table.getColumns();
        for (int i = 0; i < columns.length; i++) {
            String name = columns[i].getName().toLowerCase();
            if (!excludedColumns.contains(name)) {
                includedColumns.add(name);
            }
        }
    }

    private IDbDialect getDbDialect(SymmetricEngine engine) {
        return (IDbDialect) engine.getApplicationContext().getBean(Constants.DB_DIALECT);
    }

    private IConfigurationService getConfigurationService(SymmetricEngine engine) {
        return (IConfigurationService) engine.getApplicationContext().getBean(Constants.CONFIG_SERVICE);
    }

    private INodeService getNodeService(SymmetricEngine engine) {
        return (INodeService) engine.getApplicationContext().getBean(Constants.NODE_SERVICE);
    }

    private IDataService getDataService(SymmetricEngine engine) {
        return (IDataService) engine.getApplicationContext().getBean(Constants.DATA_SERVICE);
    }

}
