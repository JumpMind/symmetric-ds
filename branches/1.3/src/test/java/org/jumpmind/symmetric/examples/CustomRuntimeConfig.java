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

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.jumpmind.symmetric.SymmetricEngine;
import org.jumpmind.symmetric.config.IRuntimeConfig;

public class CustomRuntimeConfig {

    /**
     * Setup runtime properties in code before starting the engine. 
     */
    public static void main(String[] args) throws Exception {
        System.setProperty("symmetric.runtime.configuration.class", MyRuntimeConfig.class.getName());
        SymmetricEngine engine = new SymmetricEngine();
        engine.start();
    }

    class MyRuntimeConfig implements IRuntimeConfig {

        public String getExternalId() {
            return getHostName();
        }

        public String getMyUrl() {
            return "http://" + getHostName() + "/sync";
        }

        public String getNodeGroupId() {
            return "remote-db";
        }

        public String getRegistrationUrl() {
            return "http://mycompany.com/sync";
        }

        public String getSchemaVersion() {
            return "5.0.0";
        }

        private String getHostName() {
            try {
                return InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                return "localhost";
            }
        }
    }

}
