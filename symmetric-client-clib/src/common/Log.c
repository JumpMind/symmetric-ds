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
#include "common/Log.h"

static int SymLog_logLevel = SYM_LOG_LEVEL_DEBUG;
static unsigned short SymLog_showSourceFile = 0;
static char* SymLog_destination = "console";

static char* SymLog_getlogLevelDescription(SymLogLevel logLevel) {
	switch (logLevel) {
	case SYM_LOG_LEVEL_DEBUG:
		return SYM_LOG_LEVEL_DESC_DEBUG;
	case SYM_LOG_LEVEL_INFO:
		return SYM_LOG_LEVEL_DESC_INFO;
	case SYM_LOG_LEVEL_WARN:
		return SYM_LOG_LEVEL_DESC_WARN;
	case SYM_LOG_LEVEL_ERROR:
		return SYM_LOG_LEVEL_DESC_ERROR;
	default:
		return SYM_LOG_LEVEL_DESC_UNKNOWN;
	}
}

static SymLogLevel SymLog_getLogLevelValue(char* logLevelDescription) {
    if (SymStringUtils_equals(logLevelDescription, SYM_LOG_LEVEL_DESC_DEBUG)) {
        return SYM_LOG_LEVEL_DEBUG;
    } else if (SymStringUtils_equals(logLevelDescription, SYM_LOG_LEVEL_DESC_INFO)) {
        return SYM_LOG_LEVEL_INFO;
    } else if (SymStringUtils_equals(logLevelDescription, SYM_LOG_LEVEL_DESC_WARN)) {
        return SYM_LOG_LEVEL_WARN;
    } else if (SymStringUtils_equals(logLevelDescription, SYM_LOG_LEVEL_DESC_ERROR)) {
        return SYM_LOG_LEVEL_ERROR;
    }
    return SYM_LOG_LEVEL_DEBUG;
}

void SymLog_configure(SymProperties *settings) {
    char *logLevelDescription = settings->get(settings, SYM_LOG_SETTINGS_LOG_LEVEL, "DEBUG");
    if (! SymStringUtils_isBlank(logLevelDescription)) {
        SymLog_logLevel = SymLog_getLogLevelValue(logLevelDescription);
    }

    char *logDestination = settings->get(settings, SYM_LOG_SETTINGS_LOG_DESTINATION, "console");
    if (! SymStringUtils_isBlank(logDestination)) {
        SymLog_destination = logDestination;
    }

    char *showSourceFile = settings->get(settings, SYM_LOG_SETTINGS_LOG_SHOW_SOURCE_FILE, "0");
    if (! SymStringUtils_isBlank(showSourceFile)) {
        SymLog_showSourceFile = SymStringUtils_equals(showSourceFile, "1")
                || SymStringUtils_equalsIgnoreCase(showSourceFile, "true");
    }
}

/** This is the central place where all logging funnels through. */
void SymLog_log(SymLogLevel logLevel, const char *functionName, const char *fileName, int lineNumber, const char* message, ...) {
    if (logLevel < SymLog_logLevel) {
        return;
    }

	char* levelDescription = SymLog_getlogLevelDescription(logLevel);

	SymStringBuilder *messageBuffer = SymStringBuilder_new();

	SymDate *date = SymDate_new();

	messageBuffer->append(messageBuffer, date->dateTimeString);
	messageBuffer->append(messageBuffer, " [");
	messageBuffer->append(messageBuffer, levelDescription);
	messageBuffer->append(messageBuffer, "] [");
	messageBuffer->append(messageBuffer, functionName);
	messageBuffer->append(messageBuffer, "] ");
    va_list varargs;
    va_start(varargs, message);
    messageBuffer->appendfv(messageBuffer, message, varargs);
    va_end(varargs);
    if (SymLog_showSourceFile) {
        messageBuffer->append(messageBuffer, " (");
        messageBuffer->append(messageBuffer, fileName);
        messageBuffer->append(messageBuffer, ":");
        messageBuffer->appendInt(messageBuffer, lineNumber);
        messageBuffer->append(messageBuffer, ")");
    }
	messageBuffer->append(messageBuffer, "\n");

    unsigned short physicalFile = 0;
    FILE *destination;

    if (SymStringUtils_equalsIgnoreCase(SymLog_destination, SYM_LOG_DESTINATION_CONSOLE)) {
        if (logLevel <= SYM_LOG_LEVEL_INFO) {
            destination = stdout;
        }
        else {
            destination = stderr;
        }
    }
    else {
        destination = fopen(SymLog_destination, "a+");
        if (!destination) {
            printf("Failed to open log file destination '%s'.  Check the path our use 'console'\n", SymLog_destination);
            destination = stdout;
        }
        else  {
            physicalFile = 1;
        }

    }

	fprintf(destination, "%s", messageBuffer->toString(messageBuffer));

	// stdout may not flush before stderr does.
	// Do this to keep log messages more or less in order.
	fflush(destination);
	if (physicalFile) {
	    fclose(destination);
	}

	date->destroy(date);
	messageBuffer->destroy(messageBuffer);
}

