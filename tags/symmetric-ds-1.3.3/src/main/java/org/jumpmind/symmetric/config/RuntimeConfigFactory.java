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

import org.springframework.beans.factory.FactoryBean;

public class RuntimeConfigFactory implements FactoryBean {

    private String className;

    private IRuntimeConfig defaultInstance;

    public Object getObject() throws Exception {
        if (className != null && className.trim().length() > 0) {
            return Class.forName(className).newInstance();
        } else {
            return defaultInstance;
        }
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public void setDefaultInstance(IRuntimeConfig defaultInstance) {
        this.defaultInstance = defaultInstance;
    }

    public Class<IRuntimeConfig> getObjectType() {
        return IRuntimeConfig.class;
    }

    public boolean isSingleton() {
        return true;
    }

}
