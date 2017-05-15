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

    int trailingWhitespaces = length-1;
    if (leadingWhitespaces == length) { // all whitespace.
        trailingWhitespaces = 0;
    }
    else {
        while (isspace(str[trailingWhitespaces]) && trailingWhitespaces >=0) {
            trailingWhitespaces--;
        }

        trailingWhitespaces = ((length-1)-trailingWhitespaces);
    }

    int newLength = length - leadingWhitespaces - trailingWhitespaces;
    char *newString = malloc((newLength+1)*sizeof(char));

    int i;
    for (i = leadingWhitespaces; i < length-trailingWhitespaces; i++) {
        newString[i-leadingWhitespaces] = str[i];
    }

    newString[i] = '\0';

    return newString;
}

char *SymStringUtils_ltrim(char *str) {
    int length = strlen(str);

    int leadingWhitespaces = 0;
    while (isspace(str[leadingWhitespaces]) && leadingWhitespaces < length) {
        leadingWhitespaces++;
    }

    int newLength = length - leadingWhitespaces;
    char *newString = malloc((newLength+1)*sizeof(char));

    int i;
    for (i = leadingWhitespaces; i < length; i++) {
        newString[i-leadingWhitespaces] = str[i];
    }

    newString[newLength] = '\0';

    return newString;
}

char *SymStringUtils_rtrim(char *str) {
    int length = strlen(str);

    int trailingWhitespaces = length-1;
    while (isspace(str[trailingWhitespaces]) && trailingWhitespaces >=0) {
        trailingWhitespaces--;
    }

    trailingWhitespaces = ((length-1)-trailingWhitespaces);

    int newLength = length - trailingWhitespaces;
    char *newString = malloc((newLength+1)*sizeof(char));

    int i;
    for (i = 0; i < length-trailingWhitespaces; i++) {
        newString[i] = str[i];
    }

    newString[i] = '\0';

    return newString;
}


char *SymStringUtils_toUpperCase(char *str) {
    int length = strlen(str);
    char *newString = malloc((length+1)*sizeof(char));

    int i;
    for (i = 0; i < length; i++) {
        newString[i] = toupper(str[i]);
    }

    newString[i] = '\0';

    return newString;
}

char *SymStringUtils_toLowerCase(char *str) {
    int length = strlen(str);
    char *newString = malloc((length+1)*sizeof(char));

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

char * SymStringUtils_replaceWithLength(char *str, size_t length, char *searchFor, char* replaceWith) {
    int searchLength = strlen(searchFor);
    int replaceLength = strlen(replaceWith);
    int replaceCount = 0;
    char *index = strstr(str, searchFor);

    while (index) {
        replaceCount++;
        index += searchLength;
        index = strstr(index, searchFor);
    }

    char *newString = malloc(sizeof(char) * (length+(replaceCount*(replaceLength-searchLength))));
    char *newStringFinal = newString; // remember original pointer.

    int i;
    for (i = 0; i < replaceCount; ++i) {
        index = strstr(str, searchFor);
        int leadingChars = index - str;
        newString = strncpy(newString, str, leadingChars) + leadingChars;
        newString = strcpy(newString, replaceWith) + replaceLength;
        str += leadingChars + searchLength; // move the pointer to the end of this replacement.
    }
    strcpy(newString, str); // get remaining chars at the end.

    return newStringFinal;
}

char * SymStringUtils_replace(char *str, char *searchFor, char* replaceWith) {
    return SymStringUtils_replaceWithLength(str, strlen(str), searchFor, replaceWith);
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

void SymStringUtils_replaceChar(char *str, char oldChar, char newChar) {
    int i, len = strlen(str);
    for (i = 0; i < len; i++) {
        if (str[i] == oldChar) {
            str[i] = newChar;
        }
    }
}

unsigned short SymStringUtils_startsWith(char *str, char *prefix) {
    if (!str || !prefix)
        return 0;

    size_t length = strlen(str);
    size_t prefixLength = strlen(prefix);
    if (prefixLength >  length)
        return 0;
    return strncmp(str, prefix, prefixLength) == 0;

    return 0;
}

unsigned short SymStringUtils_endsWith(char *str, char *suffix) {
    if (!str || !suffix)
        return 0;

    size_t length = strlen(str);
    size_t suffixLength = strlen(suffix);
    if (suffixLength >  length)
        return 0;
    return strncmp(str + length - suffixLength, suffix, suffixLength) == 0;

    return 0;
}

unsigned short SymStringUtils_isNumeric(char *str) {
    int length, i;
    length = strlen (str);
    for (i=0;i<length; i++) {
        if (!isdigit(str[i])) {
            return 0;
        }
    }
    return 1;
}




