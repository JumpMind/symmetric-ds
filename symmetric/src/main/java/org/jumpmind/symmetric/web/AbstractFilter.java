package org.jumpmind.symmetric.web;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * All symmetric filters (other than {@link SymmetricFilter}) should extend this class.
 * It it managed by Spring.   
 * @author keithnaas@users.sourceforge.net
 *
 */
public abstract class AbstractFilter implements Filter {

	private ServletContext servletContext;

	private boolean disabled;

	private String uriPattern;

	private String regexPattern;

	public void init(FilterConfig filterConfig) throws ServletException {
		servletContext = filterConfig.getServletContext();
	}

	protected ServletContext getServletContext() {
		return servletContext;
	}

	public void destroy() {

	}

	protected ApplicationContext getContext() {
		return WebApplicationContextUtils
				.getWebApplicationContext(getServletContext());
	}

	public void setDisabled(boolean disabled) {
		this.disabled = disabled;
	}

	public void setUriPattern(String uriPattern) {
		this.uriPattern = uriPattern;
	}

	public boolean isDisabled() {
		return disabled;
	}

	/**
	 * Returns true if the request path matches the uriPattern on this filter.
	 * 
	 * @param request
	 * @return
	 */
	public boolean matches(ServletRequest request) {
		boolean retVal = true;
		if (request instanceof HttpServletRequest) {
			final HttpServletRequest httpRequest = (HttpServletRequest) request;
			if (uriPattern != null) {
			    retVal = matchesUriPattern(httpRequest.getServletPath());
			} else if (regexPattern != null) {
			    retVal = matchesRegexPattern(httpRequest.getServletPath());
			}
		}
		return retVal;
	}

	private boolean matchesRegexPattern(String servletPath) {
		return servletPath.matches(regexPattern);
	}

	private boolean matchesUriPattern(String servletPath) {
		boolean retVal = false;
		String path = StringUtils.defaultIfEmpty(servletPath, "/");
		final String pattern = StringUtils.defaultIfEmpty(uriPattern, "/");
		if ("/".equals(pattern) || "/*".equals(pattern)
				|| pattern.equals(path)) {
			retVal = true;
		} else {
			final String[] patternParts = StringUtils.split(pattern, "/");
			final String[] pathParts = StringUtils.split(path, "/");
			for (int i = 0; i < patternParts.length && i < pathParts.length
					&& retVal; i++) {
				final String patternPart = patternParts[i];
				retVal = "*".equals(patternPart)
						|| patternPart.equals(pathParts[i]);
			}
		}
		return retVal;
	}

	public void setRegexPattern(String regexPattern) {
		this.regexPattern = regexPattern;
	}

	/**
	 * Because you can't send an error when the response is already committed, this
	 * helps to avoid unnecessary errors in the logs. 
	 * @param resp
	 * @param statusCode
	 * @throws IOException
	 */
	protected void sendError(ServletResponse resp, int statusCode) throws IOException {
		sendError(resp, statusCode, null);
	}
	
	/**
	 * Because you can't send an error when the response is already committed, this
	 * helps to avoid unnecessary errors in the logs. 
	 * TODO: if a filter fails, should it call reset() on the response to clear the
	 * headers?
	 * @param resp
	 * @param statusCode
	 * @param message a message to put in the body of the response
	 * @throws IOException
	 */
	protected void sendError(ServletResponse resp, int statusCode, String message) throws IOException {
	    
		if (!resp.isCommitted()) {
			((HttpServletResponse)resp).sendError(statusCode, message);
		}
	}
}
