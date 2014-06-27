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

package org.jumpmind.symmetric.util;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.StatementCreatorUtils;
import org.springframework.jdbc.support.lob.LobHandler;

public class ArgTypePreparedStatementSetter implements PreparedStatementSetter
{
    private final Object[] args;

    private final int[] argTypes;
    
    private final LobHandler lobHandler;

    public ArgTypePreparedStatementSetter(Object[] args, int[] argTypes, LobHandler lobHandler) {
        this.args = args;
        this.argTypes = argTypes;
        this.lobHandler = lobHandler;
    }

    public void setValues(PreparedStatement ps) throws SQLException {
        for (int i = 1; i <= args.length; i++) {
            Object arg = args[i-1];
            int argType = argTypes[i-1];
            if (argType == Types.BLOB && lobHandler != null) {
                lobHandler.getLobCreator().setBlobAsBytes(ps, i, (byte[]) arg);
            }
            else if (argType == Types.CLOB && lobHandler != null) {
                lobHandler.getLobCreator().setClobAsString(ps, i, (String) arg);
            }
            else {
                StatementCreatorUtils.setParameterValue(ps, i, argType, arg);
            }
        }
    }

    public void cleanupParameters() {
        StatementCreatorUtils.cleanupParameters(args);
    }
}
