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
#include "util/FileUtils.h"

#ifdef SYM_WIN32
static int SymFileUtils_makeDirectory(const char *filename) {
    return CreateDirectory(filename, NULL);
}
#else
static int SymFileUtils_makeDirectory(char *filename) {
    if (SymStringUtils_equals(filename, ".") || SymStringUtils_equals(filename, "..")) {
        return 0;
    } else {
        return mkdir(filename, S_IRWXU);
    }
}
#endif

void _SymFileUtils_listFilesWithList(char *baseDirName, char* relativeDir, SymList *list, unsigned short recursive) {

    if (relativeDir == NULL) {
        relativeDir = ".";
    }
    char *dirName = SymStringUtils_format("%s/%s", baseDirName, relativeDir);
    DIR *baseDir = opendir(dirName);

    struct dirent *entry;
    if (baseDir != NULL) {
        while ((entry = readdir (baseDir)) != NULL) {
            if (entry->d_type == DT_DIR) {
                if (SymStringUtils_equals(".", entry->d_name)
                        || SymStringUtils_equals("..", entry->d_name)) {
                    continue;
                }

                SymLog_debug("Directory: %s", entry->d_name);
                SymFileEntry *fileEntry = SymFileEntry_new(NULL);
                fileEntry->fileType = entry->d_type;
                fileEntry->directory = relativeDir;
                fileEntry->fileName = SymStringUtils_format("%s", entry->d_name);  // Clone the file name as this memory will get reused.
                list->add(list, fileEntry);
                if (recursive) {
                    SymLog_debug("recurse into %s", fileEntry->fileName);
                    _SymFileUtils_listFilesWithList(baseDirName, fileEntry->fileName, list, recursive);
                }
            } else if (entry->d_type == DT_REG) {
                char *fullPathFileName = SymStringUtils_format("%s/%s", dirName, entry->d_name);

                struct stat fileInfo;
                if (stat(fullPathFileName, &fileInfo) == 0) {

                    SymDate *lastModificationTime = SymDate_newWithTime(fileInfo.st_mtime);
                    SymLog_debug("  File: %s/%s, %dB, %s", dirName, entry->d_name,
                            fileInfo.st_size,
                            lastModificationTime->dateTimeString);

                    SymFileEntry *fileEntry = SymFileEntry_new(NULL);
                    fileEntry->fileType = entry->d_type;
                    fileEntry->directory = relativeDir;
                    fileEntry->fileName = SymStringUtils_format("%s", entry->d_name);
                    fileEntry->fileModificationTime = lastModificationTime;
                    fileEntry->fileSize = fileInfo.st_size;
                    list->add(list, fileEntry);
                }

                free(fullPathFileName);

            }
        }

        (void) closedir (baseDir);
    }
    else {
        perror ("Couldn't open the directory");
    }
}

int SymFileUtils_mkdir(char* dirName) {

    int result = 0;
    struct stat st = {0};

    if (stat(dirName, &st) == -1) {
        char tmp[256];
        char *p = NULL;
        size_t len;
        char SEPERATOR = '/';

        snprintf(tmp, sizeof(tmp),"%s", dirName);
        len = strlen(tmp);
        if (tmp[len - 1] == SEPERATOR) {
            tmp[len - 1] = 0;
        }

        for (p = tmp + 1; *p; p++) {
            if(*p == '/') {
                *p = 0;
                if (!SymFileUtils_exists(tmp)) {
                    result = SymFileUtils_makeDirectory(tmp);
                    if (result != 0) {
                        SymLog_warn("Failed to create dir '%s' %s", tmp, strerror(errno));
                    }
                }
                *p = '/';
            }
        }

        if (!SymFileUtils_exists(tmp)) {
            result = SymFileUtils_makeDirectory(tmp);
            if (result != 0) {
                SymLog_warn("Failed to create dir '%s' %s", tmp, strerror(errno));
            }
        }
    }

    return result;
}

int SymFileUtils_getFileSize(char *filename) {
    struct stat st;

    if (stat(filename, &st) == 0) {
        return st.st_size;
    }

    return -1;
}

time_t SymFileUtils_getFileLastModified(char *filename) {
    struct stat attrib;
    stat(filename, &attrib);
    return attrib.st_mtime;
}

unsigned short SymFileUtils_exists(char *filename) {
    if( access( filename, F_OK ) != -1 ) {
        return 1;
    } else {
        return 0;
    }
}

SymList* SymFileUtils_listFiles(char* dirName) {
    SymList *list = SymList_new(NULL);
    _SymFileUtils_listFilesWithList(dirName, NULL, list, 0);
    return list;
}

unsigned short SymFileUtils_isDir(char *pathname) {
    struct stat ps;
    if (stat(pathname, &ps) != 0) {
        return 0;
    }
    return S_ISDIR(ps.st_mode);
}

unsigned short SymFileUtils_isRegularFile(char *pathname) {
    struct stat ps;
    if (stat(pathname, &ps) != 0) {
        return 0;
    }
    return S_ISREG(ps.st_mode);
}

SymList* SymFileUtils_listFilesRecursive(char* dirName) {
    SymList *list = SymList_new(NULL);
    _SymFileUtils_listFilesWithList(dirName, NULL, list, 1);
    return list;
}

SymStringArray* SymFileUtils_readLines(char *pathname) {
    char *fileContentsRaw = SymFileUtils_readFile(pathname);
    SymStringArray *fileLines = SymStringArray_split(fileContentsRaw, "\n");
    free(fileContentsRaw);
    return fileLines;
}

char* SymFileUtils_readFile(char *pathname) {
    FILE *file;
    int BUFFER_SIZE = 1024;
    char inputBuffer[BUFFER_SIZE];

    file = fopen(pathname, "r");
    if (!file) {
        SymLog_warn("Could not open file %s", pathname);
        return NULL;
    }

    SymStringBuilder *buff = SymStringBuilder_newWithSize(1024);

    while (fgets(inputBuffer, BUFFER_SIZE, file) != NULL) {
        buff->append(buff, inputBuffer);
    }

    char *fileContentsRaw = buff->destroyAndReturn(buff);
    fclose(file);
    return fileContentsRaw;
}

long SymFileUtils_sizeOfDirectory(char *pathname) {
    long totalSize = 0;
    SymList * /*<char*>*/ files = SymFileUtils_listFilesRecursive(pathname);
    int i;
    for (i = 0; i < files->size; ++i) {
        char *path = files->get(files, i);
        int size = SymFileUtils_getFileSize(path);
        totalSize += size;
    }
    return totalSize;
}

int SymFileUtils_deleteDir(char *dirName) {
    char* cmd = SymStringUtils_format("rm -rf %s", dirName);
    system(cmd);
    free(cmd);

    return 0;
}


void SymFileUtils_mkdirForZipEntry(char* baseDir, char* entryName) {
    int entryNameLength = strlen(entryName);

    if (entryName[entryNameLength-1] == '/') {
        char *fullDir = SymStringUtils_format("%s/%s", baseDir, entryName);
        SymFileUtils_mkdir(fullDir);
        free(fullDir);
    } else {
        SymStringArray *pathComponents = SymStringArray_split(entryName, "/");

        if (pathComponents->size == 1) {
            SymFileUtils_mkdir(baseDir);
        } else {
            SymStringBuilder *buff = SymStringBuilder_new(NULL);
            buff->append(buff, baseDir)->append(buff, "/");
            int i;
            for (i = 0; i < pathComponents->size - 1; ++i) {
                buff->append(buff, pathComponents->get(pathComponents, i))->append(buff, "/");
            }
            char *path = buff->destroyAndReturn(buff);
            SymFileUtils_mkdir(path);
            free(path);
        }

        pathComponents->destroy(pathComponents);
    }
}

int SymFileUtils_unzip(char* archive, char* destintation) {
    struct zip *zip;
    struct zip_file *zipFile;
    struct zip_stat zipEntry;
    int BUFFER_SIZE = 2048;
    char buff[BUFFER_SIZE];
    int err;
    int i, len;
    long long bytesRead;

    if ((zip = zip_open(archive, 0, &err)) == NULL) {
        zip_error_to_str(buff, sizeof(buff), err, errno);
        SymLog_error("Can't open zip archive '%s': %s", archive, buff);
        return 1;
    }

    SymFileUtils_deleteDir(destintation);

    for (i = 0; i < zip_get_num_entries(zip, 0); i++) {
        if (zip_stat_index(zip, i, 0, &zipEntry) == 0) {
            len = strlen(zipEntry.name);
            if (zipEntry.name[len - 1] == '/') {
                SymFileUtils_mkdirForZipEntry(destintation, zipEntry.name);
            } else {
//                printf("CRC %d\n", zipEntry.crc);
                SymFileUtils_mkdirForZipEntry(destintation, zipEntry.name);
                zipFile = zip_fopen_index(zip, i, 0);
                if (!zipFile) {
                    SymLog_error("Failed to open zip entry '%s'", zipEntry.name);
                    return 1;
                }

                char *path = SymStringUtils_format("%s/%s", destintation, zipEntry.name);

                FILE *entryFile = fopen(path, "a+b");
                if (!entryFile) {
                    SymLog_error("Failed to open file for zip extraction '%s'", path);
                    return 1;
                }

                bytesRead = 0;
                while (bytesRead < zipEntry.size) {
                    len = zip_fread(zipFile, buff, BUFFER_SIZE);
                    if (len < 0) {
                        SymLog_error("Failed to read entry from zip '%s'", zipEntry.name);
                        return 1;
                    }
                    fwrite(buff, sizeof(char), len, entryFile);
                    bytesRead += len;
                }

                if (bytesRead != zipEntry.size) {
                    SymLog_warn("Bytes read (%lld) doesn't match zipEntry.size (%lld) for zipEntry %s", bytesRead, zipEntry.size, zipEntry.name);
                }

                fclose(entryFile);
                zip_fclose(zipFile);
                SymFileUtils_setFileModificationTime(path, zipEntry.mtime);
                free(path);
            }
        }
    }

    zip_close(zip);
    return 0;
}

int SymFileUtils_setFileModificationTime(char *filename, time_t modificationTime) {
    struct stat fileStat;
    struct utimbuf new_times;

    stat(filename, &fileStat);

//    new_times.actime = fileStat.st_atime; /* keep atime unchanged */
    new_times.modtime = modificationTime;
    utime(filename, &new_times);

    return 0;
}


