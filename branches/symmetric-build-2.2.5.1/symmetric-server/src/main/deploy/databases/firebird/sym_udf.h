#include "stdio.h"
#include "ibase.h"
#include "ib_util.h"

static char backslash_chr = '\\';
static char quote_chr = '"';
static char *empty_str = "";

char *sym_escape(char *str);
char *sym_hex(BLOBCALLBACK blob);
