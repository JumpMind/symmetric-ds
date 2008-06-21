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

/**
 * This interface is used to pull the runtime configuration for this
 * SymmetricDSinstallation.
 * 
 * If the registrationURL is null, then this server will not register with
 * another server (it is likely that it is the host itself).
 * 
 * This interface is meant to be 'pluggable.' It might be that different
 * installations might want to pull this information from different places.
 */
@Deprecated
public interface IRuntimeConfig {

    /**
     * Get the group id for this instance
     */
    public String getNodeGroupId();

    /**
     * Get the external id for this instance
     */
    public String getExternalId();

    /**
     * Provide the URL used to register at to get initial configuration
     * information
     */
    public String getRegistrationUrl();

    /**
     * Provide the URL of this specific instance of SymmetricDS
     */
    public String getMyUrl();

    /**
     * Provide information about the version of the schema being sync'd.
     */
    public String getSchemaVersion();

}
