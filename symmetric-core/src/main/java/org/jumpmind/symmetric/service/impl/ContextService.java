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
package org.jumpmind.symmetric.service.impl;

import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.service.IContextService;
import org.jumpmind.symmetric.service.IParameterService;

// TODO: sqlite dialect should use this
public class ContextService extends AbstractService implements IContextService {

    public ContextService(IParameterService parameterService, ISymmetricDialect dialect) {
        super(parameterService, dialect);
        setSqlMap(new ContextServiceSqlMap(symmetricDialect.getPlatform(), createSqlReplacementTokens()));
    }

    public boolean is(String name) {
        return Boolean.parseBoolean(getString(name));
    }

    public int getInt(String name) {
        return Integer.parseInt(getString(name));
    }

    public long getLong(String name) {
        return Long.parseLong(getString(name));
    }

    public String getString(String name) {
        return sqlTemplate.queryForString(getSql("selectSql"), name);
    }

    public void save(String name, String value) {
        int count = sqlTemplate.update(getSql("updateSql"), value, name);
        if (count == 0) {
            sqlTemplate.update(getSql("insertSql"), name, value);
        }
    }

}
