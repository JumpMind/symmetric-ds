package org.jumpmind.symmetric.web;

import javax.servlet.Servlet;

import org.jumpmind.symmetric.ext.IExtensionPoint;

/**
 * This extension point allows additional Servlets to be registered with SymmetricDS. 
 */
public interface IServletExtension extends IExtensionPoint {

    public Servlet getServlet();
    
    public String[] getUriPatterns();
    
    public int getInitOrder();
    
    public boolean isDisabled();
    
}
