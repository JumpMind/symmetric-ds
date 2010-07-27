/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>,
 *               Andrew Wilcox <andrewbwilcox@users.sourceforge.net>,
 *               Chris Henson <chenson42@users.sourceforge.net>
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

package org.jumpmind.symmetric.security;

import org.jumpmind.symmetric.ext.IExtensionPoint;

/**
 * Used to intercept the saving and rendering of the node password.
 * @author jkrajewski
 */
public interface INodePasswordFilter extends IExtensionPoint {

	/**
	 * Called on when the node security password is being saved
	 * to the dB.
	 * @param password - The password being saved
	 */
	public String onNodeSecuritySave(String password);
	
	/**
	 * Called on when the password has been
	 * selected from the dB.
	 * @param password - The password to be used
	 */
	public String onNodeSecurityRender(String password);
}
