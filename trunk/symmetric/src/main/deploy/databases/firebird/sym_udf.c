/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

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
   int hex_result_size;
   long bytes_read;
   long bytes_left, total_bytes_read;
   int i, j;

   bytes_read = 0;
   total_bytes_read = 0;

   if (blob->blob_handle == NULL)
   {
      result = (char *) ib_util_malloc(1);
   }
   else
   {
      result = (char *) ib_util_malloc(blob->blob_total_length + 1);
      memset(result, 0, blob->blob_total_length + 1);

      bytes_left = blob->blob_total_length;
      while (bytes_left > 0)
      {
         if (!blob->blob_get_segment(blob->blob_handle, result + total_bytes_read,
                 blob->blob_total_length, &bytes_read))
         {
            break;
         }

         total_bytes_read += bytes_read;
         bytes_left -= bytes_read;
      }
   }

   hex_result_size = (total_bytes_read * 2) + 1;
   hex_result = (char *) ib_util_malloc(hex_result_size);
   memset(hex_result, 0, hex_result_size);
   for (i = 0, j = 0; j < hex_result_size; i++, j += 2)
   {
      sprintf(hex_result + j, "%02x", result[i]); 
   }

   hex_result[hex_result_size - 1] = NULL;

   return hex_result;
}
