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
package org.jumpmind.symmetric.android;

import java.io.File;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.service.impl.FileSyncService;

import android.os.Environment;
import bsh.EvalError;
import bsh.Interpreter;

public class AndroidFileSyncService extends FileSyncService {
    
    private AndroidSymmetricEngine androidEngine;

    public AndroidFileSyncService(ISymmetricEngine engine) {
        super(engine);
        if (!(engine instanceof AndroidSymmetricEngine)) {
            throw new SymmetricException("AndroidFileSyncService only works with AndroidSymmetricEngine but was given " + engine);
        }
        androidEngine = (AndroidSymmetricEngine) engine;
    }
    
    @Override
    protected void setInterpreterVariables(ISymmetricEngine engine, String sourceNodeId, File batchDir, Interpreter interpreter) throws EvalError {
        super.setInterpreterVariables(engine, sourceNodeId, batchDir, interpreter);
        interpreter.set("androidBaseDir", Environment.getExternalStorageDirectory().getPath());
        interpreter.set("androidAppFilesDir", androidEngine.androidContext.getFilesDir().getPath());
    }
 
    @Override
    protected String getEffectiveBaseDir(String baseDir) {
        String effectiveBaseDir = super.getEffectiveBaseDir(baseDir);
        if (effectiveBaseDir != null && effectiveBaseDir.startsWith("$")) {
            effectiveBaseDir = effectiveBaseDir.replace("${androidBaseDir}", Environment.getExternalStorageDirectory().getPath());
            effectiveBaseDir = effectiveBaseDir.replace("${androidAppFilesDir}", androidEngine.androidContext.getFilesDir().getPath());
            
        }
        return effectiveBaseDir;
    } 
}
