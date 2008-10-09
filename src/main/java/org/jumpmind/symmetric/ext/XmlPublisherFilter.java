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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.load.IDataLoader;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.IncomingBatchHistory;

/**
 * This is an optional data loader filter/listener that is capable of
 * translating table data to XML and publishing it to JMS for consumption by the
 * enterprise.
 * </p>
 * This class must be configured in the same context that SymmetricDS is running
 * in. Simply inject the IDataLoaderService and it will register itself with the
 * SymmetricDS engine.
 */
public class XmlPublisherFilter implements IPublisherFilter, INodeGroupExtensionPoint {

    private static final Log logger = LogFactory.getLog(XmlPublisherFilter.class);

    private static final String XML_CACHE = "XML_CACHE";

    protected IPublisher publisher;

    private Set<String> tableNamesToPublishAsGroup;

    private String xmlTagNameToUseForGroup = "batch";

    private List<String> groupByColumnNames;

    private String[] nodeGroups;

    private boolean loadDataInTargetDatabase = true;

    private boolean autoRegister = true;

    public boolean isAutoRegister() {
        return autoRegister;
    }

    public String[] getNodeGroupIdsToApplyTo() {
        return nodeGroups;
    }

    public boolean filterDelete(IDataLoaderContext ctx, String[] keys) {
        if (tableNamesToPublishAsGroup == null || tableNamesToPublishAsGroup.contains(ctx.getTableName())) {
            StringBuilder xml = getXmlFromCache(ctx, null, keys);
            if (xml != null) {
                toXmlElement(DataEventType.UPDATE, xml, ctx, null, keys);
            }
        }
        return loadDataInTargetDatabase;
    }

    public boolean filterUpdate(IDataLoaderContext ctx, String[] data, String[] keys) {
        if (tableNamesToPublishAsGroup == null || tableNamesToPublishAsGroup.contains(ctx.getTableName())) {
            StringBuilder xml = getXmlFromCache(ctx, data, keys);
            if (xml != null) {
                toXmlElement(DataEventType.UPDATE, xml, ctx, data, keys);
            }
        }
        return loadDataInTargetDatabase;
    }

    public boolean filterInsert(IDataLoaderContext ctx, String[] data) {
        if (tableNamesToPublishAsGroup == null || tableNamesToPublishAsGroup.contains(ctx.getTableName())) {
            StringBuilder xml = getXmlFromCache(ctx, data, null);
            if (xml != null) {
                toXmlElement(DataEventType.INSERT, xml, ctx, data, null);
            }
        }
        return loadDataInTargetDatabase;
    }

    private StringBuilder getXmlFromCache(IDataLoaderContext ctx, String[] data, String[] keys) {
        StringBuilder xml = null;
        Map<String, StringBuilder> ctxCache = getXmlCache(ctx);
        String txId = toXmlGroupId(ctx, data, keys);
        if (txId != null) {
            xml = ctxCache.get(txId);
            if (xml == null) {
                xml = new StringBuilder();
                xml.append("<");
                xml.append(xmlTagNameToUseForGroup);
                xml.append(" xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' id='");
                xml.append(txId);
                xml.append("' ");
                addFormattedExtraGroupAttributes(ctx, xml);
                xml.append(">");
                ctxCache.put(txId, xml);
            }
        }
        return xml;
    }

    /**
     * Give the opportunity for the user of this publisher to add in additional
     * attributes. The default implementation adds in the nodeId from the
     * {@link IDataLoaderContext}.
     * 
     * @param ctx
     * @param xml
     *                append XML attributes to this buffer
     */
    protected void addFormattedExtraGroupAttributes(IDataLoaderContext ctx, StringBuilder xml) {
        xml.append("nodeid='");
        xml.append(ctx.getNodeId());
        xml.append("' time='");
        xml.append(System.currentTimeMillis());
        xml.append("'");
    }

    @SuppressWarnings("unchecked")
    protected Map<String, StringBuilder> getXmlCache(IDataLoaderContext ctx) {
        Map<String, Object> cache = ctx.getContextCache();
        Map<String, StringBuilder> xmlCache = (Map<String, StringBuilder>) cache.get(XML_CACHE);
        if (xmlCache == null) {
            xmlCache = new HashMap<String, StringBuilder>();
            cache.put(XML_CACHE, xmlCache);
        }
        return xmlCache;
    }

    @SuppressWarnings("unchecked")
    protected boolean doesXmlExistToPublish(IDataLoaderContext ctx) {
        Map<String, Object> cache = ctx.getContextCache();
        Map<String, StringBuilder> xmlCache = (Map<String, StringBuilder>) cache.get(XML_CACHE);
        return xmlCache != null && xmlCache.size() > 0;
    }

    private void toXmlElement(DataEventType dml, StringBuilder xml, IDataLoaderContext ctx, String[] data, String[] keys) {
        xml.append("<row entity='");
        xml.append(ctx.getTableName());
        xml.append("' dml='");
        xml.append(dml.getCode());
        xml.append("'>");

        String[] colNames = null;

        if (data == null) {
            colNames = ctx.getKeyNames();
            data = keys;
        } else {
            colNames = ctx.getColumnNames();
        }

        for (int i = 0; i < data.length; i++) {
            String col = colNames[i];
            xml.append("<data key='");
            xml.append(col);
            xml.append("'");

            if (data[i] == null) {
                xml.append(" xsi:nil='true'/>");
            } else {
                xml.append(">");
                xml.append(data[i]);
                xml.append("</data>");
            }
        }
        xml.append("</row>");
    }

    private String toXmlGroupId(IDataLoaderContext ctx, String[] data, String[] keys) {
        if (groupByColumnNames != null) {
            StringBuilder id = new StringBuilder();

            if (keys != null) {
                String[] columns = ctx.getKeyNames();
                for (String col : groupByColumnNames) {
                    int index = ArrayUtils.indexOf(columns, col, 0);
                    if (index >= 0) {
                        id.append(data[index]);
                    } else {
                        id = new StringBuilder();
                        break;
                    }
                }
            }

            if (id.length() == 0) {
                String[] columns = ctx.getColumnNames();
                for (String col : groupByColumnNames) {
                    int index = ArrayUtils.indexOf(columns, col, 0);
                    if (index >= 0) {
                        id.append(data[index]);
                    } else {
                        return null;
                    }
                }
            }

            if (id.length() > 0) {
                return id.toString().replaceAll("-", "");
            }
        }
        return null;
    }

    private void finalizeXmlAndPublish(IDataLoaderContext ctx) {
        Map<String, StringBuilder> ctxCache = getXmlCache(ctx);
        Collection<StringBuilder> buffers = ctxCache.values();
        for (Iterator<StringBuilder> iterator = buffers.iterator(); iterator.hasNext();) {
            StringBuilder xml = iterator.next();
            xml.append("</");
            xml.append(xmlTagNameToUseForGroup);
            xml.append(">");
            if (logger.isDebugEnabled()) {
                logger.debug("Sending XML to IPublisher -> " + xml);
            }
            iterator.remove();
            publisher.publish(ctx, xml.toString());            
        }
        
    }

    public void batchComplete(IDataLoader loader, IncomingBatchHistory hist) {
        IDataLoaderContext ctx = loader.getContext();
        if (doesXmlExistToPublish(ctx)) {
            finalizeXmlAndPublish(ctx);
        }
    }

    public void setTableNamesToPublishAsGroup(Set<String> tableNamesToPublishAsGroup) {
        this.tableNamesToPublishAsGroup = tableNamesToPublishAsGroup;
    }

    public void setXmlTagNameToUseForGroup(String xmlTagNameToUseForGroup) {
        this.xmlTagNameToUseForGroup = xmlTagNameToUseForGroup;
    }

    public void setGroupByColumnNames(List<String> groupByColumnNames) {
        this.groupByColumnNames = groupByColumnNames;
    }

    public void setLoadDataInTargetDatabase(boolean loadDataInTargetDatabase) {
        this.loadDataInTargetDatabase = loadDataInTargetDatabase;
    }

    public void setPublisher(IPublisher publisher) {
        this.publisher = publisher;
    }

    public void setNodeGroups(String[] nodeGroups) {
        this.nodeGroups = nodeGroups;
    }

    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }

}
