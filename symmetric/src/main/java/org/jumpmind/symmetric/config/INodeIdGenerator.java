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
package org.jumpmind.symmetric.config;

import org.jumpmind.symmetric.ext.IExtensionPoint;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.INodeService;

/**
 * An {@link IExtensionPoint} that allows SymmetricDS users to implement their
 * own algorithms for how node_ids and passwords are generated or selected
 * during registration.
 */
public interface INodeIdGenerator extends IExtensionPoint {

    /**
     * Based on the node parameters passed in generate an expected node id. This
     * is used in an attempt to match a registration request.
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
