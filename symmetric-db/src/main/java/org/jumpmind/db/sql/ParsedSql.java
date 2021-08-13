/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.db.sql;

import java.util.ArrayList;
import java.util.List;

/**
 * Holds information about a parsed SQL statement.
 *
 * @author Thomas Risberg
 * @author Juergen Hoeller
 * @since 2.0
 */
public class ParsedSql {
    private String originalSql;
    private List<String> parameterNames = new ArrayList<String>();
    private List<int[]> parameterIndexes = new ArrayList<int[]>();
    private int namedParameterCount;
    private int unnamedParameterCount;
    private int totalParameterCount;

    /**
     * Create a new instance of the {@link ParsedSql} class.
     * 
     * @param originalSql
     *            the SQL statement that is being (or is to be) parsed
     */
    ParsedSql(String originalSql) {
        this.originalSql = originalSql;
    }

    /**
     * Return the SQL statement that is being parsed.
     */
    String getOriginalSql() {
        return this.originalSql;
    }

    /**
     * Add a named parameter parsed from this SQL statement.
     * 
     * @param parameterName
     *            the name of the parameter
     * @param startIndex
     *            the start index in the original SQL String
     * @param endIndex
     *            the end index in the original SQL String
     */
    void addNamedParameter(String parameterName, int startIndex, int endIndex) {
        this.parameterNames.add(parameterName);
        this.parameterIndexes.add(new int[] { startIndex, endIndex });
    }

    /**
     * Return all of the parameters (bind variables) in the parsed SQL statement. Repeated occurences of the same parameter name are included here.
     */
    List<String> getParameterNames() {
        return this.parameterNames;
    }

    /**
     * Return the parameter indexes for the specified parameter.
     * 
     * @param parameterPosition
     *            the position of the parameter (as index in the parameter names List)
     * @return the start index and end index, combined into a int array of length 2
     */
    int[] getParameterIndexes(int parameterPosition) {
        return this.parameterIndexes.get(parameterPosition);
    }

    /**
     * Set the count of named parameters in the SQL statement. Each parameter name counts once; repeated occurences do not count here.
     */
    void setNamedParameterCount(int namedParameterCount) {
        this.namedParameterCount = namedParameterCount;
    }

    /**
     * Return the count of named parameters in the SQL statement. Each parameter name counts once; repeated occurences do not count here.
     */
    int getNamedParameterCount() {
        return this.namedParameterCount;
    }

    /**
     * Set the count of all of the unnamed parameters in the SQL statement.
     */
    void setUnnamedParameterCount(int unnamedParameterCount) {
        this.unnamedParameterCount = unnamedParameterCount;
    }

    /**
     * Return the count of all of the unnamed parameters in the SQL statement.
     */
    int getUnnamedParameterCount() {
        return this.unnamedParameterCount;
    }

    /**
     * Set the total count of all of the parameters in the SQL statement. Repeated occurences of the same parameter name do count here.
     */
    void setTotalParameterCount(int totalParameterCount) {
        this.totalParameterCount = totalParameterCount;
    }

    /**
     * Return the total count of all of the parameters in the SQL statement. Repeated occurences of the same parameter name do count here.
     */
    int getTotalParameterCount() {
        return this.totalParameterCount;
    }

    /**
     * Exposes the original SQL String.
     */
    @Override
    public String toString() {
        return this.originalSql;
    }
}
