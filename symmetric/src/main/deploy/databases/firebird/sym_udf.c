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

#include <stdio.h>
#include "ib_util.h"

static char backslash_chr = '\\';
static char quote_chr = '"';
static char *empty_str = "";

char *sym_escape(char *str);

char *sym_escape(char *str)
{
   int len = 0, count = 0;
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

   char *result = (char *) ib_util_malloc(count + len + 1);
   if (result == NULL)
   {
      return empty_str;
   }

   int i, j;
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

