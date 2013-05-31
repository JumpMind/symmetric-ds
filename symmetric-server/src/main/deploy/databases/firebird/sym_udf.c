#include "sym_udf.h"

char *sym_escape(char *str)
{
   int len = 0, count = 0;
   int i, j;
   char *result;

   if (str != NULL)
   {
      for (; str[len] != NULL; len++)
      {
         if (str[len] == quote_chr || str[len] == backslash_chr)
         {
            count++;
         }
      }
   }

   result = (char *) ib_util_malloc(count + len + 1);
   if (result == NULL)
   {
      return empty_str;
   }

   for (i = 0, j = 0; i < len; i++, j++)
   {
      if (str[i] == quote_chr || str[i] == backslash_chr)
      {
         result[j] = backslash_chr;
         result[++j] = str[i];
      }
      else
      {
         result[j] = str[i];
      }
   }
   result[j] = NULL;

   return result;
}

/*
 * Code for reading the BLOB was kindly borrowed from FreeAdhocUDF, written by
 * adhoc dataservice GmbH, Christoph Theuring ct / Peter Mandrella pm / Georg Horn gh
 */
char *sym_hex(BLOBCALLBACK blob)
{
   char *result, *hex_result;
   long hex_result_size;
   long bytes_read;
   long bytes_left, total_bytes_read;
   long i, j;

   bytes_read = 0;
   total_bytes_read = 0;

   if (blob->blob_handle == NULL)
   {
      result = (char *) malloc(1);
   }
   else
   {
      result = (char *) malloc(blob->blob_total_length + 1);
      memset(result, 0, blob->blob_total_length + 1);

      bytes_left = blob->blob_total_length;
      while (bytes_left > 0)
      {
         if (!blob->blob_get_segment(blob->blob_handle, (char *)result + total_bytes_read,
                 blob->blob_total_length, &bytes_read))
         {
            break;
         }

         total_bytes_read += bytes_read;
         bytes_left -= bytes_read;
      }
   }
   
   while (total_bytes_read>0 && isspace(result[total_bytes_read-1])) 
   {
    --total_bytes_read;
   }

   result[total_bytes_read] = '\0';
  
   hex_result_size = (total_bytes_read * 2) + 1;
   hex_result = (char *) ib_util_malloc(hex_result_size);
   memset(hex_result, 0, hex_result_size);
   for (i = 0, j = 0; i < total_bytes_read; i++, j += 2)
   {
     sprintf(hex_result + j, "%02x", (unsigned char)result[i]); 
   }

   hex_result[hex_result_size] = '\0';    
  
   free(result);

   return hex_result;
}
