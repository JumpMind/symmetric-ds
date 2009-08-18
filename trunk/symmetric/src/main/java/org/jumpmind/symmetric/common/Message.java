/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Mark Hanes <eegeek@users.sourceforge.net>
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
package org.jumpmind.symmetric.common;

import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

public class Message {

    private static Locale locale = null;

    private static String bundleName = "messages";

    private static ResourceBundle bundle = null;
    private static String MESSAGE_KEY = "MessageKey: ";

    static {
        setLocale(Locale.getDefault());
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.jumpmind.symmetric.common.logging.ILog#getMessage(java.lang.String)
     */
    public static String get(String key) {
        return get(key, (Object) null);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.jumpmind.symmetric.common.logging.ILog#getMessage(java.lang.String,
     * java.lang.Object)
     */
    public static String get(String key, Object... args) {
        if (bundle != null) {
            try {
                if (args != null) {
                    return String.format(locale, bundle.getString(key), args);
                } else {
                    return String.format(locale, bundle.getString(key));
                }
            } catch (RuntimeException e) {
                return MESSAGE_KEY + key + ((args != null) ? args.toString() : "");
            }
        } else {
            return MESSAGE_KEY + key + ((args != null) ? args.toString() : "");
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jumpmind.symmetric.common.logging.ILog#getLocale()
     */
    public static Locale getLocale() {
        return locale;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.jumpmind.symmetric.common.logging.ILog#setLocale(java.util.Locale)
     */
    public static void setLocale(Locale locale) {
        Message.locale = locale;
        try {
            Message.bundle = ResourceBundle.getBundle(bundleName, locale);
        } catch (MissingResourceException e) {
            Message.bundle = null;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jumpmind.symmetric.common.logging.ILog#getBundleName()
     */
    public static String getBundleName() {
        return bundleName;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.jumpmind.symmetric.common.logging.ILog#setBundleName(java.lang.String
     * )
     */
    public static void setBundleName(String bundleName) {
        Message.bundleName = bundleName;
        Message.bundle = ResourceBundle.getBundle(bundleName, locale);
    }
}
