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
package org.jumpmind.symmetric.service;

import java.util.List;
import java.util.Map;

import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.ext.ExtensionPointMetaData;
import org.jumpmind.symmetric.model.Extension;

public interface IExtensionService {
    public void refresh();

    public List<ExtensionPointMetaData> getExtensionPointMetaData();

    public <T extends IExtensionPoint> T getExtensionPoint(Class<T> extensionClass);

    public <T extends IExtensionPoint> List<T> getExtensionPointList(Class<T> extensionClass);

    public <T extends IExtensionPoint> Map<String, T> getExtensionPointMap(Class<T> extensionClass);

    public void addExtensionPoint(IExtensionPoint extension);

    public void addExtensionPoint(String name, IExtensionPoint extension);

    public void removeExtensionPoint(IExtensionPoint extension);

    public List<Extension> getExtensions();

    public void saveExtension(Extension extension);

    public void deleteExtension(String extensionId);

    public Object getCompiledClass(String javaCode) throws Exception;
}
