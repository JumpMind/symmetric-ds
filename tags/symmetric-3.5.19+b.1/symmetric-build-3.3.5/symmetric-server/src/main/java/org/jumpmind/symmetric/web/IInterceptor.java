package org.jumpmind.symmetric.web;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public interface IInterceptor {

    public boolean before(HttpServletRequest req, HttpServletResponse res) throws IOException,
            ServletException;
    
    public void after(HttpServletRequest req, HttpServletResponse res) throws IOException,
    ServletException;    

}
