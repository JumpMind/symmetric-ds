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
 
#include <stdio.h>
#include <stdarg.h>
#include "common/Log.h"
#include "util/StringBuilder.h"
#include <time.h>

char* logLevelDescription(LogLevel logLevel) {
	switch (logLevel) {
	case DEBUG:
		return "DEBUG";
	case INFO:
		return "INFO";
	case WARN:
		return "WARN";
	case ERROR:
		return "ERROR";
	default:
		return "<UNKNOWN>";
	}
}

char *generateTimestamp() {
	int TIMESTAMP_LENGTH = 26;
    time_t timer;
    char* buffer = malloc(TIMESTAMP_LENGTH);
    // char buffer[26];
    struct tm* tm_info;

    time(&timer);
    tm_info = localtime(&timer);

    strftime(buffer, TIMESTAMP_LENGTH, "%Y-%m-%dT%H:%M:%S", tm_info);
    return buffer;
}

/** This is the central place where all logging funnels through. */
void SymLog_log(LogLevel logLevel, const char *functionName, const char *fileName, int lineNumber, const char* message, ...) {
	FILE *destination;
	if (logLevel <= INFO) {
		destination = stdout;
	}
	else {
		destination = stderr;
	}

	char* levelDescription = logLevelDescription(logLevel);

	SymStringBuilder *messageBuffer = SymStringBuilder_new();

	char* logTimestamp = generateTimestamp();

	messageBuffer->append(messageBuffer, logTimestamp);
	messageBuffer->append(messageBuffer, " [");
	messageBuffer->append(messageBuffer, levelDescription);
	messageBuffer->append(messageBuffer, "] [");
	messageBuffer->append(messageBuffer, functionName);
//	messageBuffer->append(messageBuffer, " ");
//	messageBuffer->append(messageBuffer, fileName);
//	messageBuffer->append(messageBuffer, ":");
//	messageBuffer->appendInt(messageBuffer, lineNumber);
	messageBuffer->append(messageBuffer, "] ");
    va_list varargs;
    va_start(varargs, message);
	messageBuffer->appendfv(messageBuffer, message, varargs);
    va_end(varargs);

	messageBuffer->append(messageBuffer, "\n");

	fprintf(destination, "%s", messageBuffer->toString(messageBuffer));

	// stdout may not flush before stderr does.
	// Do this to keep log messages more or less in order.
	fflush(destination);

	messageBuffer->destroy(messageBuffer);
	free(logTimestamp);
}

