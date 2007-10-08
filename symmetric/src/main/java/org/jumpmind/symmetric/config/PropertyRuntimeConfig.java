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

import java.util.Properties;

public class PropertyRuntimeConfig implements IRuntimeConfig {

    String groupId;

    String externalId;

    String registrationUrlString;

    String myUrlString;
    
    String schemaVersion;

    Properties properties = new Properties();

    public String getNodeGroupId() {
        return groupId;
    }

    public String getRegistrationUrl() {
        return registrationUrlString;
    }

    public Properties getParameters() {
        return properties;
    }

    public void setGroupId(String domainName) {
        this.groupId = domainName;
    }

    public void setRegistrationUrlString(String registrationUrlString) {
        this.registrationUrlString = registrationUrlString;
    }

    public String getExternalId() {
        return externalId;
    }

    public void setExternalId(String domainId) {
        this.externalId = domainId;
    }

    public void setMyUrlString(String myUrlString) {
        this.myUrlString = myUrlString;
    }

    public String getMyUrl() {
        return myUrlString;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(String schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

}
