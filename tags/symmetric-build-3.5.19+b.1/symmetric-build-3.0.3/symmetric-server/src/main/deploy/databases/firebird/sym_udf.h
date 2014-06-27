#include "stdio.h"
#include "ibase.h"
#include "ib_util.h"

static char backslash_chr = '\\';
static char quote_chr = '"';
static char *empty_str = "";

typedef struct blob {
	short	(*blob_get_segment) ();
	void	*blob_handle;
	long	blob_number_segments;
	long	blob_max_segment;
	long	blob_total_length;
	void	(*blob_put_segment) ();
} *BLOBCALLBACK;

char *sym_escape(char *str);
char *sym_hex(BLOBCALLBACK blob);
