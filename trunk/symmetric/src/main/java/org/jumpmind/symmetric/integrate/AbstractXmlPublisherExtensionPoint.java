package org.jumpmind.symmetric.integrate;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.Namespace;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;
import org.jumpmind.symmetric.ext.ICacheContext;
import org.jumpmind.symmetric.ext.IExtensionPoint;
import org.jumpmind.symmetric.ext.INodeGroupExtensionPoint;

public class AbstractXmlPublisherExtensionPoint implements IExtensionPoint, INodeGroupExtensionPoint {

    protected final Log logger = LogFactory.getLog(getClass());

    protected final String XML_CACHE = "XML_CACHE_" + this.hashCode();

    private String[] nodeGroups;

    private boolean autoRegister = true;

    protected IPublisher publisher;

    protected Set<String> tableNamesToPublishAsGroup;

    protected String xmlTagNameToUseForGroup = "batch";

    protected List<String> groupByColumnNames;

    protected Format xmlFormat;

    protected ITimeGenerator timeStringGenerator = new ITimeGenerator() {
        public String getTime() {
            return Long.toString(System.currentTimeMillis());
        }
    };

    public AbstractXmlPublisherExtensionPoint() {
        xmlFormat = Format.getCompactFormat();
        xmlFormat.setOmitDeclaration(true);
    }

    protected final static Namespace getXmlNamespace() {
        return Namespace.getNamespace("xsi", "http://www.w3.org/2001/XMLSchema-instance");
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Element> getXmlCache(ICacheContext ctx) {
        Map<String, Object> cache = ctx.getContextCache();
        Map<String, Element> xmlCache = (Map<String, Element>) cache.get(XML_CACHE);
        if (xmlCache == null) {
            xmlCache = new HashMap<String, Element>();
            cache.put(XML_CACHE, xmlCache);
        }
        return xmlCache;
    }

    @SuppressWarnings("unchecked")
    protected boolean doesXmlExistToPublish(ICacheContext ctx) {
        Map<String, Object> cache = ctx.getContextCache();
        Map<String, StringBuilder> xmlCache = (Map<String, StringBuilder>) cache.get(XML_CACHE);
        return xmlCache != null && xmlCache.size() > 0;
    }

    protected void finalizeXmlAndPublish(ICacheContext ctx) {
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

    public boolean isAutoRegister() {
        return autoRegister;
    }

    public String[] getNodeGroupIdsToApplyTo() {
        return nodeGroups;
    }

    public void setNodeGroups(String[] nodeGroups) {
        this.nodeGroups = nodeGroups;
    }

    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }

    public void setPublisher(IPublisher publisher) {
        this.publisher = publisher;
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
     * This attribute is required. It needs to identify the columns that will be used to key on rows in the specified
     * tables that need to be grouped together in an 'XML batch.'
     */
    public void setGroupByColumnNames(List<String> groupByColumnNames) {
        this.groupByColumnNames = groupByColumnNames;
    }

    public interface ITimeGenerator {
        public String getTime();
    }

}
