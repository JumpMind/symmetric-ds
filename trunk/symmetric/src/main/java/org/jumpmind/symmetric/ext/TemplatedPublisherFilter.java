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
package org.jumpmind.symmetric.ext;

import java.text.ParseException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.load.IDataLoader;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.IDataLoaderFilter;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;
import org.jumpmind.symmetric.model.IncomingBatchHistory;
import org.springframework.beans.factory.BeanNameAware;

/**
 * A convenience class that allows the end user to template a message using
 * SymmetricDS filter data.
 * </p>
 * You may use %COLUMN% formatted tokens in your template data which will be
 * replaced by data coming in through the filter. The following tokens are also
 * supported:
 * <ol>
 * <li>%DMLTYPE% - evaluates to INSERT, UPDATE or DELETE</li>
 * <li>%TIMESTAMP% - evaluates to ms value returned by
 * System.currentTimeInMillis()</li>
 * </ol>
 * </p>
 * If you have special formatting needs, implement the {@link IFormat} interface
 * and map your formatter to the column you want to 'massage.'
 */
public class TemplatedPublisherFilter extends AbstractTextPublisherFilter implements INodeGroupExtensionPoint,
        BeanNameAware {

    static final Log logger = LogFactory.getLog(TemplatedPublisherFilter.class);

    private Map<String, String> headerTableTemplates;
    private Map<String, String> footerTableTemplates;
    private Map<String, String> contentTableTemplates;
    private Map<String, IFormat> columnNameToDataFormatter;
    private String[] nodeGroupIdsToApplyTo;
    private boolean processDelete = true;
    private boolean processInsert = true;
    private boolean processUpdate = true;
    private String beanName;
    private int messagesSinceLastLogOutput = 0;
    private long minTimeInMsBetweenLogOutput = 30000;
    private long lastTimeInMsOutputLogged = System.currentTimeMillis();
    private IDataLoaderFilter dataFilter;

    public String[] getNodeGroupIdsToApplyTo() {
        return nodeGroupIdsToApplyTo;
    }

    public void setBeanName(String name) {
        this.beanName = name;
    }

    public void setNodeGroupIdToApplyTo(String nodeGroupdId) {
        this.nodeGroupIdsToApplyTo = new String[] { nodeGroupdId };
    }

    @Override
    public void batchComplete(IDataLoader loader, IncomingBatchHistory hist) {
        super.batchComplete(loader, hist);
        if (doesTextExistToPublish(loader.getContext())) {
            logCount();
        }
    }

    protected void logCount() {
        messagesSinceLastLogOutput++;
        long timeInMsSinceLastLogOutput = System.currentTimeMillis() - lastTimeInMsOutputLogged;
        if (timeInMsSinceLastLogOutput > minTimeInMsBetweenLogOutput) {
            if (logger.isInfoEnabled()) {
                logger.info(beanName + " published " + messagesSinceLastLogOutput + " messages in the last "
                        + timeInMsSinceLastLogOutput + "ms");
            }
            lastTimeInMsOutputLogged = System.currentTimeMillis();
            messagesSinceLastLogOutput = 0;
        }
    }

    @Override
    protected String addTextElementForDelete(IDataLoaderContext ctx, String[] keys) {
        if (this.dataFilter == null || this.dataFilter.filterDelete(ctx, keys)) {
            String template = null;
            if (processDelete) {
                template = contentTableTemplates.get(ctx.getTableName());
                if (template != null) {
                    template = fillOutTemplate(DmlType.DELETE, template, ctx, null, keys);
                }
            }
            return template;
        } else {
            return null;
        }
    }

    @Override
    protected String addTextElementForInsert(IDataLoaderContext ctx, String[] data) {
        if (this.dataFilter == null || this.dataFilter.filterInsert(ctx, data)) {
            String template = null;
            if (processInsert) {
                template = contentTableTemplates.get(ctx.getTableName());
                if (template != null) {
                    template = fillOutTemplate(DmlType.INSERT, template, ctx, data, null);
                }
            }
            return template;
        } else {
            return null;
        }
    }

    @Override
    protected String addTextElementForUpdate(IDataLoaderContext ctx, String[] data, String[] keys) {
        if (this.dataFilter == null || this.dataFilter.filterUpdate(ctx, data, keys)) {
            String template = null;
            if (processUpdate) {
                template = contentTableTemplates.get(ctx.getTableName());
                if (template != null) {
                    template = fillOutTemplate(DmlType.UPDATE, template, ctx, data, keys);
                }
            }
            return template;
        } else {
            return null;
        }
    }

    @Override
    protected String addTextFooter(IDataLoaderContext ctx) {
        return footerTableTemplates.get(ctx.getTableName());
    }

    @Override
    protected String addTextHeader(IDataLoaderContext ctx) {
        return headerTableTemplates.get(ctx.getTableName());
    }

    protected String fillOutTemplate(DmlType dmlType, String template, IDataLoaderContext ctx, String[] data,
            String[] keys) {
        String[] colNames = null;

        if (data == null) {
            colNames = ctx.getKeyNames();
            data = keys;
        } else {
            colNames = ctx.getColumnNames();
        }

        for (int i = 0; i < data.length; i++) {
            String col = colNames[i];
            template = replace(template, col, format(col, data[i]));
        }

        template = template.replace("DMLTYPE", dmlType.name());
        template = template.replace("TIMESTAMP", Long.toString(System.currentTimeMillis()));

        return template;
    }

    protected String format(String col, String data) {
        if (columnNameToDataFormatter != null) {
            IFormat formatter = columnNameToDataFormatter.get(col);
            if (formatter != null) {
                try {
                    data = formatter.format(data);
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return data;
    }

    protected String replace(String template, String token, String value) {
        if (template != null && value != null) {
            template = template.replace("%" + token + "%", value);
        }
        return template;
    }

    public void setColumnNameToDataFormatter(Map<String, IFormat> columnNameToDataFormatter) {
        this.columnNameToDataFormatter = columnNameToDataFormatter;
    }

    public void setProcessDelete(boolean processDeletes) {
        this.processDelete = processDeletes;
    }

    public void setProcessInsert(boolean processInserts) {
        this.processInsert = processInserts;
    }

    public void setProcessUpdate(boolean processUpdates) {
        this.processUpdate = processUpdates;
    }

    public void setHeaderTableTemplates(Map<String, String> headerTableTemplates) {
        this.headerTableTemplates = headerTableTemplates;
    }

    public void setFooterTableTemplates(Map<String, String> footerTableTemplates) {
        this.footerTableTemplates = footerTableTemplates;
    }

    public void setContentTableTemplates(Map<String, String> contentTableTemplates) {
        this.contentTableTemplates = contentTableTemplates;
    }

    public void setNodeGroupIdsToApplyTo(String[] nodeGroupsToApplyTo) {
        this.nodeGroupIdsToApplyTo = nodeGroupsToApplyTo;
    }

    public void setMessagesSinceLastLogOutput(int messagesSinceLastLogOutput) {
        this.messagesSinceLastLogOutput = messagesSinceLastLogOutput;
    }

    public void setMinTimeInMsBetweenLogOutput(long timeInMsBetweenLogOutput) {
        this.minTimeInMsBetweenLogOutput = timeInMsBetweenLogOutput;
    }

    public interface IFormat {
        public String format(String data) throws ParseException;
    }

    public void setDataFilter(IDataLoaderFilter dataFilter) {
        this.dataFilter = dataFilter;
    }

}
