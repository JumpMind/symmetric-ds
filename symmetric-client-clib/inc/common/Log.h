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

typedef enum {DEBUG, INFO, WARN, ERROR} LogLevel;

#define SymLog_debug(M, ...) SymLog_log(0, __func__, __FILE__, __LINE__, M, ##__VA_ARGS__)
#define SymLog_info(M, ...) SymLog_log(1, __func__, __FILE__, __LINE__, M, ##__VA_ARGS__)
#define SymLog_warn(M, ...) SymLog_log(2, __func__, __FILE__, __LINE__, M, ##__VA_ARGS__)
#define SymLog_error(M, ...) SymLog_log(3, __func__, __FILE__, __LINE__, M, ##__VA_ARGS__)

void SymLog_log(LogLevel logLevel, const char *functionName, const char *filename, int lineNumber, const char* message, ...);
