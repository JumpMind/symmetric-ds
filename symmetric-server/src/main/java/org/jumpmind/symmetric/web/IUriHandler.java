package org.jumpmind.symmetric.web;

import java.io.IOException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileUploadException;

public interface IUriHandler {
    
    public void handle(HttpServletRequest req, HttpServletResponse res) throws IOException, ServletException, FileUploadException;
    
    public String getUriPattern();
    
    public List<IInterceptor> getInterceptors();
    
    public boolean isEnabled();

}
