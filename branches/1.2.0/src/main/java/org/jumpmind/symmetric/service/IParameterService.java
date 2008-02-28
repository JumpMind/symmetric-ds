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
import java.util.Map;

import org.jumpmind.symmetric.model.GlobalParameter;

/**
 * Get and set application wide configuration information.
 * @author chenson
 */
public interface IParameterService {

    public String getString(String configurationId, GlobalParameter key);

    public int getInt(String configurationId, GlobalParameter key);

    public BigDecimal getDecimal(String configurationId, GlobalParameter key);

    public long getLong(String configurationId, GlobalParameter key);

    public void saveParameter(String configurationId, String key, Object param);

    public void saveParameters(String configurationId,
            Map<String, Object> parameters);

    public void populateDefautGlobalParametersIfNeeded();
}
