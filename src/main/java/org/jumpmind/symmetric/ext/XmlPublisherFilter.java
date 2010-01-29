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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.Namespace;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;
import org.jumpmind.symmetric.load.IDataLoader;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.IncomingBatchHistory;

/**
 * This is an optional data loader filter/listener that is capable of
 * translating table data to XML and publishing it to JMS for consumption by the
 * enterprise. It uses JDOM internally to create an XML representation of
 * SymmetricDS data.
 * </p>
 */
public class XmlPublisherFilter implements IPublisherFilter, INodeGroupExtensionPoint {

    private static final Log logger = LogFactory.getLog(XmlPublisherFilter.class);

    private final String XML_CACHE = "XML_CACHE_" + this.hashCode();

    protected IPublisher publisher;

    private Set<String> tableNamesToPublishAsGroup;

    private String xmlTagNameToUseForGroup = "batch";

    private List<String> groupByColumnNames;

    private String[] nodeGroups;

    private boolean loadDataInTargetDatabase = true;

    private boolean autoRegister = true;
    
    private Format xmlFormat;

    private ITimeGenerator timeStringGenerator = new ITimeGenerator() {
        public String getTime() {
            return Long.toString(System.currentTimeMillis());
        }
    };
    
    public XmlPublisherFilter() {
        xmlFormat = Format.getCompactFormat();
        xmlFormat.setOmitDeclaration(true);
    }

    public boolean isAutoRegister() {
        return autoRegister;
    }

    public String[] getNodeGroupIdsToApplyTo() {
        return nodeGroups;
    }

    public boolean filterDelete(IDataLoaderContext ctx, String[] keys) {
        if (tableNamesToPublishAsGroup == null || tableNamesToPublishAsGroup.contains(ctx.getTableName())) {
            Element xml = getXmlFromCache(ctx, null, keys);
            if (xml != null) {
                toXmlElement(DataEventType.UPDATE, xml, ctx, null, keys);
            }
        }
        return loadDataInTargetDatabase;
    }

    public boolean filterUpdate(IDataLoaderContext ctx, String[] data, String[] keys) {
        if (tableNamesToPublishAsGroup == null || tableNamesToPublishAsGroup.contains(ctx.getTableName())) {
            Element xml = getXmlFromCache(ctx, data, keys);
            if (xml != null) {
                toXmlElement(DataEventType.UPDATE, xml, ctx, data, keys);
            }
        }
        return loadDataInTargetDatabase;
    }

    public boolean filterInsert(IDataLoaderContext ctx, String[] data) {
        if (tableNamesToPublishAsGroup == null || tableNamesToPublishAsGroup.contains(ctx.getTableName())) {
            Element xml = getXmlFromCache(ctx, data, null);
            if (xml != null) {
                toXmlElement(DataEventType.INSERT, xml, ctx, data, null);
            }
        }
        return loadDataInTargetDatabase;
    }

    private Element getXmlFromCache(IDataLoaderContext ctx, String[] data, String[] keys) {
        Element xml = null;
        Map<String, Element> ctxCache = getXmlCache(ctx);
        String txId = toXmlGroupId(ctx, data, keys);
        if (txId != null) {
            xml = ctxCache.get(txId);
            if (xml == null) {
                xml = new Element(xmlTagNameToUseForGroup);
                xml.addNamespaceDeclaration(getXmlNamespace());
                xml.setAttribute("id", txId);
                addFormattedExtraGroupAttributes(ctx, xml);
                ctxCache.put(txId, xml);
            }
        }
        return xml;
    }

    private final static Namespace getXmlNamespace() {
        return Namespace.getNamespace("xsi", "http://www.w3.org/2001/XMLSchema-instance");
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
    protected void addFormattedExtraGroupAttributes(IDataLoaderContext ctx, Element xml) {
        xml.setAttribute("nodeid", ctx.getNodeId());
        if (timeStringGenerator != null) {
            xml.setAttribute("time", timeStringGenerator.getTime());
        }
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Element> getXmlCache(IDataLoaderContext ctx) {
        Map<String, Object> cache = ctx.getContextCache();
        Map<String, Element> xmlCache = (Map<String, Element>) cache.get(XML_CACHE);
        if (xmlCache == null) {
            xmlCache = new HashMap<String, Element>();
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

    private void toXmlElement(DataEventType dml, Element xml, IDataLoaderContext ctx, String[] data, String[] keys) {
        Element row = new Element("row");
        xml.addContent(row);
        row.setAttribute("entity", ctx.getTableName());
        row.setAttribute("dml", dml.getCode());

        String[] colNames = null;

        if (data == null) {
            colNames = ctx.getKeyNames();
            data = keys;
        } else {
            colNames = ctx.getColumnNames();
        }

        for (int i = 0; i < data.length; i++) {
            String col = colNames[i];
            Element dataElement = new Element("data");
            row.addContent(dataElement);
            dataElement.setAttribute("key", col);
            if (data[i] != null) {
                dataElement.setText(data[i]);
            } else {
                dataElement.setAttribute("nil", "true", getXmlNamespace());
            }
        }
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
        } else {
            logger.warn("You did not specify 'groupByColumnNames'.  We cannot find any matches in the data to publish as XML if you don't.  You might as well turn off this filter!");
        }
        return null;
    }

    private void finalizeXmlAndPublish(IDataLoaderContext ctx) {
        Map<String, Element> ctxCache = getXmlCache(ctx);
        Collection<Element> buffers = ctxCache.values();
        for (Iterator<Element> iterator = buffers.iterator(); iterator.hasNext();) {
            String xml = new XMLOutputter(xmlFormat).outputString(new Document(iterator.next()));
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
    
    public void setTableNameToPublish(String tableName) {
        this.tableNamesToPublishAsGroup = new HashSet<String>(1);
        this.tableNamesToPublishAsGroup.add(tableName);
    }

    public void setXmlTagNameToUseForGroup(String xmlTagNameToUseForGroup) {
        this.xmlTagNameToUseForGroup = xmlTagNameToUseForGroup;
    }

    /**
     * This attribute is required.  It needs to identify the columns that will be used to key on
     * rows in the specified tables that need to be grouped together in an 'XML batch.'
     */
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

    interface ITimeGenerator {
        public String getTime();
    }

    /**
     * Used to populate the time attribute of an XML message.
     */
    public void setTimeStringGenerator(ITimeGenerator timeStringGenerator) {
        this.timeStringGenerator = timeStringGenerator;
    }

    public void setXmlFormat(Format xmlFormat) {
        this.xmlFormat = xmlFormat;
    }
    
    public void batchCommitted(IDataLoader loader, IncomingBatchHistory history) {
    }
    
    public void batchRolledback(IDataLoader loader, IncomingBatchHistory history) {       
    }

}
