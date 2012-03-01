/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License. 
 */

package org.jumpmind.symmetric.util;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.StatementCreatorUtils;
import org.springframework.jdbc.support.lob.LobHandler;

/**
 * 
 */
public class ArgTypePreparedStatementSetter implements PreparedStatementSetter {
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
            Object arg = args[i - 1];
            int argType = argTypes[i - 1];
            if (argType == Types.BLOB && lobHandler != null) {
                lobHandler.getLobCreator().setBlobAsBytes(ps, i, (byte[]) arg);
            } else if (argType == Types.CLOB && lobHandler != null) {
                lobHandler.getLobCreator().setClobAsString(ps, i, (String) arg);
            } else {
                StatementCreatorUtils.setParameterValue(ps, i,
                        translateUnrecognizedArgTypes(argType), arg);
            }
        }
    }
    
    protected int translateUnrecognizedArgTypes(int argType) {
        if (argType == -101 || argType == Types.OTHER) {
            return Types.VARCHAR;
        } else {
            return argType;
        }
    }

    public void cleanupParameters() {
        StatementCreatorUtils.cleanupParameters(args);
    }
}