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

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.logging.Log;
import org.jumpmind.symmetric.common.logging.LogFactory;

/**
 * A list of properties files that were set via system properties so that if the
 * system property changes down the road the initially configured files remain
 * captured during a properties refresh.
 */
public class DynamicPropertiesFiles extends ArrayList<String> {

    private static final long serialVersionUID = 1L;
    private static Log logger = LogFactory.getLog(DynamicPropertiesFiles.class);

    public DynamicPropertiesFiles() {
        File file = new File(System.getProperty("user.dir"), "symmetric.properties");
        if (file.exists() && file.isFile()) {
            try {
                add(file.toURL().toExternalForm());
            } catch (MalformedURLException e) {
                logger.error(e);
            }
        }
        if (!StringUtils.isBlank(System.getProperty("symmetric.override.properties.file.1"))) {
            add(System.getProperty("symmetric.override.properties.file.1"));
        }
        if (!StringUtils.isBlank(System.getProperty("symmetric.override.properties.file.2"))) {
            add(System.getProperty("symmetric.override.properties.file.2"));
        }
    }
}
