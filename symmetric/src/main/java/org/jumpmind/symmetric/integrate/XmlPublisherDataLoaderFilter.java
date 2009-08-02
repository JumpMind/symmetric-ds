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
package org.jumpmind.symmetric.integrate;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.jdom.Element;
import org.jumpmind.symmetric.ext.INodeGroupExtensionPoint;
import org.jumpmind.symmetric.load.IBatchListener;
import org.jumpmind.symmetric.load.IDataLoader;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.IDataLoaderFilter;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.IncomingBatch;

/**
 * This is an optional {@link IDataLoaderFilter} and {@link IBatchListener} that is capable of translating table data to
 * XML and publishing it for consumption by the enterprise. It uses JDOM internally to create an XML representation of
 * SymmetricDS data.
 * <p>
 * This filter is typically configured as a Spring bean. The table names that should be published are identified by {@link #setTableNamesToPublishAsGroup(Set)}. Rows
 * from tables can be grouped together (which get synchronized in the same batch) by identifying columns that are the
 * same that act as a 'key' by setting {@link #setGroupByColumnNames(List)}
 * <p>
 * The {@link IPublisher} is typically configured and injected onto this bean as well.  Provided is a {@link SimpleJmsPublisher}.
 * <p>
 * An example of the XML that is published is as follows:
 * <pre>
 * &lt;batch id="2TEST2" nodeid="00001" time="12345678910"&gt;
 *   &lt;row entity="TABLE_NAME" dml="I"&gt;
 *     &lt;data key="id1"&gt;2&lt;/data&gt;
 *     &lt;data key="id2"&gt;TEST&lt;/data&gt;
 *     &lt;data key="id3"&gt;2&lt;/data&gt;
 *     &lt;data key="data1"&gt;Me&lt;/data&gt;
 *     &lt;data key="data2" nil="true"/&gt;
 *   &lt;/row&gt;
 * &lt;/batch&gt;
 * </pre>
 */
public class XmlPublisherDataLoaderFilter extends AbstractXmlPublisherExtensionPoint implements IPublisherFilter, INodeGroupExtensionPoint {
    
    protected boolean loadDataInTargetDatabase = true;

    public XmlPublisherDataLoaderFilter() {
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

    public void batchComplete(IDataLoader loader, IncomingBatch batch) {
        IDataLoaderContext ctx = loader.getContext();
        if (doesXmlExistToPublish(ctx)) {
            finalizeXmlAndPublish(ctx);
        }
    }
    /**
     * Give the opportunity for the user of this publisher to add in additional attributes. The default implementation
     * adds in the nodeId from the {@link IDataLoaderContext}.
     * 
     * @param ctx
     * @param xml
     *            append XML attributes to this buffer
     */
    protected void addFormattedExtraGroupAttributes(IDataLoaderContext ctx, Element xml) {
        xml.setAttribute("nodeid", ctx.getNodeId());
        if (timeStringGenerator != null) {
            xml.setAttribute("time", timeStringGenerator.getTime());
        }
    }
    
    protected void toXmlElement(DataEventType dml, Element xml, IDataLoaderContext ctx, String[] data, String[] keys) {
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
    
    
    protected Element getXmlFromCache(IDataLoaderContext ctx, String[] data, String[] keys) {
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

    protected String toXmlGroupId(IDataLoaderContext ctx, String[] data, String[] keys) {
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
            logger
                    .warn("You did not specify 'groupByColumnNames'.  We cannot find any matches in the data to publish as XML if you don't.  You might as well turn off this filter!");
        }
        return null;
    }
    public void setLoadDataInTargetDatabase(boolean loadDataInTargetDatabase) {
        this.loadDataInTargetDatabase = loadDataInTargetDatabase;
    }


    public void batchCommitted(IDataLoader loader, IncomingBatch batch) {
    }

    public void batchRolledback(IDataLoader loader, IncomingBatch batch) {
    }
    
    public void earlyCommit(IDataLoader loader, IncomingBatch batch) {
    }

}
