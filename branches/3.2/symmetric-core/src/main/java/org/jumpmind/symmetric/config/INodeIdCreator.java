package org.jumpmind.symmetric.config;

import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.util.DefaultNodeIdCreator;

/**
 * An {@link IExtensionPoint} that allows SymmetricDS users to implement their
 * own algorithms for how node_ids and passwords are generated or selected
 * during registration.  There may be only one node creator per SymmetricDS instance.
 * </p>
 * The default implementation of this is the {@link DefaultNodeIdCreator}
 */
public interface INodeIdCreator extends IExtensionPoint {

    /**
     * Based on the node parameters passed in generate an expected node id. This
     * is used in an attempt to match a registration request with an open registration.
     */
    public String selectNodeId(Node node, String remoteHost, String remoteAddress);

    /**
     * Based on the node parameters passed in generate a brand new node id.
     */
    public String generateNodeId(Node node, String remoteHost, String remoteAddress);

    /**
     * Generate a password to use when opening registration
     */
    public String generatePassword(Node node);

}
