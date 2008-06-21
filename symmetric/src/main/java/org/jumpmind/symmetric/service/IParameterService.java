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

package org.jumpmind.symmetric.service;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;

import org.jumpmind.symmetric.config.IParameterFilter;

/**
 * Get and set application wide configuration information.
 */
public interface IParameterService {

    public static final String ALL = "ALL";

    public BigDecimal getDecimal(String key);

    public boolean is(String key);

    public int getInt(String key);

    public long getLong(String key);

    public String getString(String key);

    public void saveParameter(String key, Object paramValue);

    public void saveParameter(String nodeId, String nodeGroupId, String key, Object paramValue);

    public void saveParameters(String nodeId, String nodeGroupId, Map<String, Object> parameters);

    public void rereadParameters();

    public Date getLastTimeParameterWereCached();

    public Map<String, String> getAllParameters();

    public void setParameterFilter(IParameterFilter f);

    /**
     * Get the group id for this instance
     */
    public String getNodeGroupId();

    /**
     * Get the external id for this instance
     */
    public String getExternalId();

    /**
     * Provide the url used to register at to get initial configuration
     * information
     */
    public String getRegistrationUrl();

    /**
     * Provide information about the URL used to contact this symmetric instance
     */
    public String getMyUrl();

}
