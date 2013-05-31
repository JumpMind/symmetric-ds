package org.jumpmind.symmetric.config;

import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.util.DefaultNodeIdCreator;

/**
 * An {@link IExtensionPoint} that allows SymmetricDS users to implement their
 * own algorithms for how node_ids and passwords are generated or selected
 * during registration.  There may be only one node generator per SymmetricDS instance.
 * </p>
 * The default implementation of this is the {@link DefaultNodeIdCreator}
 */
@Deprecated
public interface INodeIdGenerator extends IExtensionPoint {

    /**
     * Based on the node parameters passed in generate an expected node id. This
     * is used in an attempt to match a registration request with an open registration.
     */
    public String selectNodeId(INodeService nodeService, Node node);

    /**
     * Based on the node parameters passed in generate a brand new node id.
     */
    public String generateNodeId(INodeService nodeService, Node node);

    /**
     * Generate a password to use when opening registration
     */
    public String generatePassword(INodeService nodeService, Node node);
}