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
#ifndef SYM_FILEUTILS_H
#define SYM_FILEUTILS_H

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <zip.h>
#include <utime.h>
#include "common/Log.h"
#include "util/StringUtils.h"
#include "util/List.h"
#include "file/FileEntry.h"
#ifdef SYM_WIN32
#include <Windows.h>
#endif

int SymFileUtils_mkdir(char* dirName);
int SymFileUtils_getFileSize(char *filename);
time_t SymFileUtils_getFileLastModified(char *filename);
unsigned short SymFileUtils_exists(char *filename);
SymList* SymFileUtils_listFiles(char* dirName); // list of SymFileEntry
SymList* SymFileUtils_listFilesRecursive(char* dirName); // list of SymFileEntry
unsigned short SymFileUtils_isRegularFile(char *pathname);
unsigned short SymFileUtils_isDir(char *pathname);
SymStringArray* SymFileUtils_readLines(char *pathname);
char* SymFileUtils_readFile(char *pathname);
long SymFileUtils_sizeOfDirectory(char *pathname);
int SymFileUtils_deleteDir(char *dirName);
int SymFileUtils_setFileModificationTime(char *filename, time_t modificationTime);
int SymFileUtils_unzip(char* archive, char* destintation);

#endif
