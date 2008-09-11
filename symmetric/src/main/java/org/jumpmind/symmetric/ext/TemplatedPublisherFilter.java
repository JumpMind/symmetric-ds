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
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;

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
 */
public class TemplatedPublisherFilter extends AbstractTextPublisherFilter {

    static final Log logger = LogFactory.getLog(TemplatedPublisherFilter.class);

    Map<String, String> headerTableTemplates;
    Map<String, String> footerTableTemplates;
    Map<String, String> contentTableTemplates;
    Map<String, IFormat> columnNameToDataFormatter;
    boolean processDelete = true;
    boolean processInsert = true;
    boolean processUpdate = true;

    @Override
    protected String addTextElementForDelete(IDataLoaderContext ctx, String[] keys) {
        String template = null;
        if (processDelete) {
            template = contentTableTemplates.get(ctx.getTableName());
            if (template != null) {
                template = fillOutTemplate(DmlType.DELETE, template, ctx, null, keys);
            }
        }
        return template;
    }

    @Override
    protected String addTextElementForInsert(IDataLoaderContext ctx, String[] data) {
        String template = null;
        if (processInsert) {
            template = contentTableTemplates.get(ctx.getTableName());
            if (template != null) {
                template = fillOutTemplate(DmlType.INSERT, template, ctx, data, null);
            }
        }
        return template;
    }

    @Override
    protected String addTextElementForUpdate(IDataLoaderContext ctx, String[] data, String[] keys) {
        String template = null;
        if (processUpdate) {
            template = contentTableTemplates.get(ctx.getTableName());
            if (template != null) {
                template = fillOutTemplate(DmlType.UPDATE, template, ctx, data, keys);
            }
        }
        return template;
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

    public interface IFormat {
        public String format(String data) throws ParseException;
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

}
