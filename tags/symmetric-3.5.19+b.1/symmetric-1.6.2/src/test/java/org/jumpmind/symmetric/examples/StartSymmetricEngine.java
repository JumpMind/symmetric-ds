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
package org.jumpmind.symmetric.examples;

import org.jumpmind.symmetric.SymmetricEngine;

public class StartSymmetricEngine {

    /**
     * Start an engine that is configured by two properties files. One is
     * packaged with the application and contains overridden properties that are
     * specific to the application. The other is found in the application's
     * working directory. It can be used to setup environment specific
     * properties.
     */
    public static void main(String[] args) throws Exception {
        String workingDirectory = System.getProperty("user.dir");

        SymmetricEngine engine = new SymmetricEngine("classpath://my-symmetric-application.properties", "file://"
                + workingDirectory + "/my-symmetric-environment.properties");

        // this will create the database, sync triggers, start jobs running
        engine.start();
    }

}
