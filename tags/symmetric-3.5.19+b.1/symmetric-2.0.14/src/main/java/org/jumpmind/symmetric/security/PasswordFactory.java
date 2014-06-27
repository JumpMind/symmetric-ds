/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>
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

package org.jumpmind.symmetric.security;

import org.jumpmind.symmetric.common.SecurityConstants;
import org.jumpmind.symmetric.service.ISecurityService;
import org.springframework.beans.factory.FactoryBean;

/**
 * Used to protect database user and password from casual observation in the properties file
 */
public class PasswordFactory implements FactoryBean<String> {

    private ISecurityService securityService;

    private String password;

    public String getObject() throws Exception {
        if (password != null && password.startsWith(SecurityConstants.PREFIX_ENC)) {
            return securityService.decrypt(password.substring(SecurityConstants.PREFIX_ENC.length()));
        }
        return password;
    }

    public Class<String> getObjectType() {
        return String.class;
    }

    public boolean isSingleton() {
        return false;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setSecurityService(ISecurityService securityService) {
        this.securityService = securityService;
    }
}