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

#ifndef SYM_LOG_H
#define SYM_LOG_H

#include <stdio.h>
#include <errno.h>
#include <time.h>
#include <libgen.h>
#include <dirent.h>
#include <sys/stat.h>
#include "util/StringBuilder.h"
#include "util/Properties.h"
#include "util/Date.h"
#include "util/StringUtils.h"
#include "util/FileUtils.h"

typedef enum {SYM_LOG_LEVEL_DEBUG, SYM_LOG_LEVEL_INFO, SYM_LOG_LEVEL_WARN, SYM_LOG_LEVEL_ERROR} SymLogLevel;

#define SYM_LOG_DESTINATION_CONSOLE "console"

#define SYM_LOG_SETTINGS_LOG_LEVEL "client.log.level"
#define SYM_LOG_SETTINGS_LOG_DESTINATION "client.log.destination"
#define SYM_LOG_SETTINGS_LOG_SHOW_SOURCE_FILE "client.log.show.source.file"
#define SYM_LOG_SETTINGS_LOG_MAX_FILE_SIZE "client.log.max.file.size"
#define SYM_LOG_SETTINGS_LOG_BACKUP_APPEND "client.log.backup.append"
#define SYM_LOG_SETTINGS_LOG_DAYS_TO_KEEP "client.log.backup.days.to.keep"

#define SYM_LOG_LEVEL_DESC_DEBUG "DEBUG"
#define SYM_LOG_LEVEL_DESC_INFO "INFO"
#define SYM_LOG_LEVEL_DESC_WARN "WARN"
#define SYM_LOG_LEVEL_DESC_ERROR "ERROR"
#define SYM_LOG_LEVEL_DESC_UNKNOWN "UNKNOWN"

#define SymLog_debug(M, ...) SymLog_log(0, __func__, __FILE__, __LINE__, M, ##__VA_ARGS__)
#define SymLog_info(M, ...) SymLog_log(1, __func__, __FILE__, __LINE__, M, ##__VA_ARGS__)
#define SymLog_warn(M, ...) SymLog_log(2, __func__, __FILE__, __LINE__, M, ##__VA_ARGS__)
#define SymLog_error(M, ...) SymLog_log(3, __func__, __FILE__, __LINE__, M, ##__VA_ARGS__)

void SymLog_log(SymLogLevel logLevel, const char *functionName, const char *filename, int lineNumber, const char* message, ...);
void SymLog_configure(SymProperties *settings);

#endif
