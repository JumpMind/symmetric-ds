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

package org.jumpmind.symmetric;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * This is the preferred way to wire a SymmetricDS instance into an existing
 * Spring context. It will create its own {@link ApplicationContext} as a child
 * of the Spring {@link ApplicationContext} it is being wired into.
 */
public class SpringWireableSymmetricEngine extends AbstractSymmetricEngine implements ApplicationContextAware {

    public SpringWireableSymmetricEngine() {
    }

    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.init(applicationContext, true, null, null);
    }

}
