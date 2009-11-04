/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Mark Hanes <eegeek@users.sourceforge.net>
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


public interface ILog {

    public abstract boolean isDebugEnabled();

    public abstract void debug(String messageKey);

    public abstract void debug(String messageKey, Throwable t);

    public abstract void debug(String messageKey, Object... args);

    public abstract void debug(String messageKey, Throwable t, Object... args);

    public abstract boolean isInfoEnabled();

    public abstract void info(String messageKey);

    public abstract void info(String messageKey, Throwable t);

    public abstract void info(String messageKey, Object... args);

    public abstract void info(String messageKey, Throwable t, Object... args);

    public abstract boolean isWarnEnabled();

    public abstract void warn(String messageKey);

    public abstract void warn(String messageKey, Throwable t);

    public abstract void warn(String messageKey, Object... args);

    public abstract void warn(String messageKey, Throwable t, Object... args);

    public abstract void warn(Throwable t);

    public abstract boolean isErrorEnabled();

    public abstract void error(String messageKey);

    public abstract void error(String messageKey, Throwable t);

    public abstract void error(String messageKey, Object... args);

    public abstract void error(String messageKey, Throwable t, Object... args);

    public abstract void error(Throwable t);

    public abstract void fatal(String messageKey);

    public abstract void fatal(String messageKey, Throwable t);

    public abstract void fatal(String messageKey, Object... args);

    public abstract void fatal(String messageKey, Throwable t, Object... args);

    public abstract void fatal(Throwable t);

}