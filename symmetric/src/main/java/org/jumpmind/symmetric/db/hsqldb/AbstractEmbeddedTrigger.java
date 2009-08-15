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
package org.jumpmind.symmetric.db.hsqldb;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.SymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.logging.Log;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.ITriggerService;
import org.jumpmind.symmetric.util.AppUtils;

/**
 * This class implements the functionality needed by (most) java-based symmetric
 * triggers.
 */
public abstract class AbstractEmbeddedTrigger {

    protected static final Log logger = LogFactory.getLog(AbstractEmbeddedTrigger.class);
    protected static final FastDateFormat dateFormatter = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.S");
    protected IDataService dataService;
    protected ITriggerService triggerService;
    protected INodeService nodeService;
    protected IParameterService parameterService;
    protected IDbDialect dbDialect;
    protected Table table;
    protected TriggerHistory triggerHistory;
    protected Trigger trigger;
    protected DataEventType triggerType;
    protected String tableName;
    protected Set<String> excludedColumns;
    protected List<String> includedColumns;

    protected boolean initialize(DataEventType triggerType, String tableName) {
        this.triggerType = triggerType;
        this.tableName = tableName;
        SymmetricEngine engine = SymmetricEngine.findEngineByName(getEngineName().toLowerCase());
        this.dataService = getDataService(engine);
        this.triggerService = getTriggerService(engine);
        this.parameterService = AppUtils.find(Constants.PARAMETER_SERVICE, engine);
        this.nodeService = getNodeService(engine);
        this.dbDialect = getDbDialect(engine);
        this.triggerHistory = triggerService.getHistoryRecordFor(getTriggerHistId());
        this.trigger = triggerService.getActiveTriggersForSourceNodeGroup(
                parameterService.getString(ParameterConstants.NODE_GROUP_ID), true).get(triggerHistory.getTriggerId());
        if (trigger == null) {
            logger.warn("TriggerMissing", triggerType.name(), tableName, getTriggerHistId());
            return false;
        }
        this.table = dbDialect.getMetaDataFor(null, trigger.getSourceSchemaName(), tableName, true);
        initColumnNames(trigger);
        return true;
    }

    protected abstract String getEngineName();

    protected abstract int getTriggerHistId();

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

    protected boolean toCsv(Object object, StringBuilder b) {
        boolean handled = true;
        if (object != null) {
            if (object instanceof String) {
                b.append("\"");
                b.append(StringUtils.replace(StringUtils.replace(object.toString(), "\\", "\\\\"), "\"", "\\\""));
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
            } else if (object instanceof Boolean) {
                b.append(((Boolean) object) ? "\"1\"" : "\"0\"");
            } else if (object instanceof Reader) { // clob in h2
                b.append("\"");
                try {
                    b.append(StringUtils.replace(StringUtils.replace(IOUtils.toString((BufferedReader) object), "\\",
                            "\\\\"), "\"", "\\\""));
                } catch (IOException ex) {
                    throw new IllegalStateException("Unable to read CLOB");
                }
                b.append("\"");
            } else if (object instanceof ByteArrayInputStream) { // blob in h2
                b.append("\"");
                try {
                    b.append(new String(Base64.encodeBase64(IOUtils.toByteArray((ByteArrayInputStream) object))));
                } catch (IOException ex) {
                    throw new IllegalStateException("Unable to read BLOB");
                }
                b.append("\"");
            } else {
                handled = false;
            }
        }
        return handled;
    }

    protected String formatAsCsv(Object[] data) {
        StringBuilder b = new StringBuilder();
        if (data != null) {
            for (Object object : data) {
                if (!toCsv(object, b)) {
                    throw new IllegalStateException("Could not format " + object + " which is of type "
                            + object.getClass().getName());
                }
                b.append(",");
            }
            b.deleteCharAt(b.length() - 1);
        }
        return b.toString();
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
                if (allValues != null && allValues.length > i) {
                    values[x++] = allValues[i];
                } else {
                    values[x++] = null;
                }
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
        return AppUtils.find(Constants.DB_DIALECT, engine);
    }

    private ITriggerService getTriggerService(SymmetricEngine engine) {
        return AppUtils.find(Constants.TRIGGER_SERVICE, engine);
    }

    private INodeService getNodeService(SymmetricEngine engine) {
        return AppUtils.find(Constants.NODE_SERVICE, engine);
    }

    private IDataService getDataService(SymmetricEngine engine) {
        return AppUtils.find(Constants.DATA_SERVICE, engine);
    }
}
