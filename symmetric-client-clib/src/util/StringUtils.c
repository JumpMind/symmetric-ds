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
#include "util/StringUtils.h"

char *SymStringUtils_trim(char *str)
{
    int length = strlen(str);

    int leadingWhitespaces = 0;
    while (isspace(str[leadingWhitespaces]) && leadingWhitespaces < length) {
        leadingWhitespaces++;
    }

    if (leadingWhitespaces == length) { // all whitespace.
        return "";
    }

    int trailingWhitespaces = length-1;
    while (isspace(str[trailingWhitespaces]) && trailingWhitespaces >=0) {
        trailingWhitespaces--;
    }

    trailingWhitespaces = ((length-1)-trailingWhitespaces);

    int newLength = length - leadingWhitespaces - trailingWhitespaces;
    char *newString = malloc(newLength+1);

    int i;
    for (i = leadingWhitespaces; i < length-trailingWhitespaces; i++) {
        newString[i-leadingWhitespaces] = str[i];
    }

    newString[i] = '\0';

    return newString;
}

char *SymStringUtils_toUpperCase(char *str) {
    int length = strlen(str);
    char *newString = malloc(length+1);

    int i;
    for (i = 0; i < length; i++) {
        newString[i] = toupper(str[i]);
    }

    newString[i] = '\0';

    return newString;
}

char *SymStringUtils_toLowerCase(char *str) {
    int length = strlen(str);
    char *newString = malloc(length+1);

    int i;
    for (i = 0; i < length; i++) {
        newString[i] = tolower(str[i]);
    }

    newString[i] = '\0';

    return newString;
}

unsigned short SymStringUtils_isBlank(char *str) {
    if (str == NULL) {
        return 1;
    }

    int strLen = strlen(str);
    if (strLen == 0) {
        return 1;
    }

    int i;
    for (i = 0; i < strLen; i++) {
        if (!isspace(str[i])) {
            return 0;
        }
    }

    return 1;
}

unsigned short SymStringUtils_isNotBlank(char *str) {
    return ! SymStringUtils_isBlank(str);
}

char* SymStringUtils_format(char *format, ...) {
    SymStringBuilder *buff = SymStringBuilder_new(NULL);

    va_list varargs;
    va_start(varargs, format);

    buff->appendfv(buff, format, varargs);

    va_end(varargs);

    return buff->destroyAndReturn(buff);
}

char * SymStringUtils_substring(char *str, int startIndex, int endIndex) {
    SymStringBuilder *sb = SymStringBuilder_newWithString(str);
    char *value = sb->substring(sb, startIndex, endIndex);
    sb->destroy(sb);
    return value;
}

unsigned short SymStringUtils_equals(char *str1, char *str2) {
    if (str1 == NULL && str2 == NULL) {
        return 1;
    } else if (str1 == NULL || str2 == NULL) {
        return 0;
    }
    return strcmp(str1, str2) == 0;
}

unsigned short SymStringUtils_equalsIgnoreCase(char *str1, char *str2) {
    if (str1 == NULL && str2 == NULL) {
        return 1;
    } else if (str1 == NULL || str2 == NULL) {
        return 0;
    }
    return strcasecmp(str1, str2) == 0;
}
