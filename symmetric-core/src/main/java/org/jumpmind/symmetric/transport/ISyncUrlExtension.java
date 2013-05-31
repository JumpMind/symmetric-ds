package org.jumpmind.symmetric.transport;

import java.net.URI;

import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.transport.http.HttpBandwidthUrlSelector;

/**
 * This {@link IExtensionPoint} is used to select an appropriate URL based on
 * the URI provided in the sync_url column of sym_node.
 * <p/>
 * To use this extension point configure the sync_url for a node with the
 * protocol of ext://beanName. The beanName is the name you give the extension
 * point in the extension xml file.
 * 
 * @see HttpBandwidthUrlSelector
 *
 * 
 */
public interface ISyncUrlExtension extends IExtensionPoint {

    public String resolveUrl(URI url);

}