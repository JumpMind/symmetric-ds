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
package org.jumpmind.symmetric.common.logging;

public class NoOpLog implements ILog {

    public void debug(String messageKey) {

    }

    public void debug(String messageKey, Throwable t) {

    }

    public void debug(String messageKey, Object... args) {

    }

    public void debug(String messageKey, Throwable t, Object... args) {

    }

    public void error(String messageKey) {

    }

    public void error(String messageKey, Throwable t) {

    }

    public void error(String messageKey, Object... args) {

    }

    public void error(String messageKey, Throwable t, Object... args) {

    }

    public void error(Throwable t) {

    }

    public void fatal(String messageKey) {

    }

    public void fatal(String messageKey, Throwable t) {

    }

    public void fatal(String messageKey, Object... args) {

    }

    public void fatal(String messageKey, Throwable t, Object... args) {

    }

    public void fatal(Throwable t) {

    }

    public void info(String messageKey) {

    }

    public void info(String messageKey, Throwable t) {

    }

    public void info(String messageKey, Object... args) {

    }

    public void info(String messageKey, Throwable t, Object... args) {

    }

    public boolean isDebugEnabled() {

        return false;
    }

    public boolean isErrorEnabled() {

        return false;
    }

    public boolean isInfoEnabled() {

        return false;
    }

    public boolean isWarnEnabled() {

        return false;
    }

    public void warn(String messageKey) {

    }

    public void warn(String messageKey, Throwable t) {

    }

    public void warn(String messageKey, Object... args) {

    }

    public void warn(String messageKey, Throwable t, Object... args) {

    }

    public void warn(Throwable t) {

    }

}
