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
package org.jumpmind.symmetric.util;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Method;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.db.ISymmetricDialect;

final public class SymmetricUtils {

    protected static boolean isJava7 = true;
    
    protected static Method copyMethod;
    
    protected static Method fileMethod; 
    
    protected static Object optionArray;

    private SymmetricUtils() {
    }
    
    public static String quote(ISymmetricDialect symmetricDialect, String name) {
        String quote = symmetricDialect.getPlatform().getDatabaseInfo().getDelimiterToken();
        if (StringUtils.isNotBlank(quote)) {
            return quote + name + quote;
        } else {
            return name;
        }
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void copyFile(File source, File target) throws IOException {
        if (isJava7) {
            try {
                if (copyMethod == null) {
                    Class filesClass = Class.forName("java.nio.file.Files");
                    Class pathClass = Class.forName("java.nio.file.Path");
                    Class optionArrayClass = Class.forName("[Ljava.nio.file.CopyOption;");
                    Class optionClass = Class.forName("java.nio.file.CopyOption");
                    Class standardOptionClass = Class.forName("java.nio.file.StandardCopyOption");
        
                    copyMethod = filesClass.getMethod("copy", new Class[] { pathClass, pathClass, optionArrayClass });
                    fileMethod = File.class.getMethod("toPath", (Class[]) null);
                    optionArray = Array.newInstance(optionClass, 1);
                    Array.set(optionArray, 0, Enum.valueOf(standardOptionClass, "REPLACE_EXISTING"));
                }

                Object sourcePath = fileMethod.invoke(source, (Object[]) null);
                Object targetPath = fileMethod.invoke(target, (Object[]) null);
                copyMethod.invoke(null, new Object[] { sourcePath, targetPath, optionArray });
                return;
            } catch (Exception e) {
                isJava7 = false;
            }
        }
        FileUtils.copyFile(source, target);
    }
}
