/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Keith Naas <knaas@users.sourceforge.net>
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

package org.jumpmind.symmetric.service.mock;

import java.io.IOException;
import java.io.OutputStream;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IRegistrationService;

public class MockRegistrationService implements IRegistrationService {

    public boolean isAutoRegistration() {
        return false;
    }

    public void openRegistration(String nodeGroupId, String externalId) {

    }

    public void reOpenRegistration(String nodeId) {

    }

    public boolean registerNode(Node node, OutputStream out) throws IOException {
        return false;
    }

    public void registerWithServer() {
    }

    public boolean isRegisteredWithServer() {
        return true;
    }
}