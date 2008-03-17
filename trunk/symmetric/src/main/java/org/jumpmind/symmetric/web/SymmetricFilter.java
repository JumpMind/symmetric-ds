package org.jumpmind.symmetric.web;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * This filter allows us simplify the configuration of symmetric by defining
 * filters directly within spring configuration files.
 * 
 * @author keithnaas@users.sourceforge.net
 * 
 */
public class SymmetricFilter implements Filter {

	private static final Log logger = LogFactory.getLog(SymmetricFilter.class);

	private ServletContext servletContext;

	private List<Filter> filters;

	public void doFilter(ServletRequest request, ServletResponse response,
			FilterChain chain) throws IOException, ServletException {
		new SymmetricFilterChain(chain).doFilter(request, response);
	}

	public void init(FilterConfig filterConfig) throws ServletException {
		servletContext = filterConfig.getServletContext();
		filters = new ArrayList<Filter>();
		@SuppressWarnings("unchecked")
		final Map<String, Filter> filterBeans = getContext().getBeansOfType(
				Filter.class);
		// they will need to be sorted somehow, right now its just the order
		// they appear in the spring file
		for (final Map.Entry<String, Filter> filterEntry : filterBeans
				.entrySet()) {
			if (logger.isInfoEnabled()) {
				logger.info(String.format("Initializing filter %s", filterEntry
						.getKey()));
			}
			final Filter filter = filterEntry.getValue();
			filter.init(filterConfig);
			filters.add(filter);
		}
	}

	public void destroy() {
		for (final Filter filter : filters) {
			filter.destroy();
		}

	}

	protected ApplicationContext getContext() {
		return WebApplicationContextUtils
				.getWebApplicationContext(getServletContext());
	}

	public ServletContext getServletContext() {
		return servletContext;
	}

	/**
	 * The chain will visit each filter in turn. When done, it will pass along
	 * to the original chain.
	 * The chain skips disabled filters.  I'm wondering if this should be moved to the
	 * {@link SymmetricFilter#init(FilterConfig)}.
	 * @author Keith
	 * 
	 */
	private class SymmetricFilterChain implements FilterChain {

		private FilterChain chain;
		private int index;

		public SymmetricFilterChain(FilterChain chain) {
			this.chain = chain;
			index = 0;
		}

		public void doFilter(ServletRequest request, ServletResponse response)
				throws IOException, ServletException {
			if (index < filters.size()) {
				final Filter filter = filters.get(index++);
				if (filter instanceof AbstractFilter) {
					final AbstractFilter builtinFilter = (AbstractFilter) filter;
					if (!builtinFilter.isDisabled()
							&& builtinFilter.matches(request)) {
						builtinFilter.doFilter(request, response, chain);
					}
				} else {
					filter.doFilter(request, response, chain);
				}
			} else {
				chain.doFilter(request, response);
			}
		}
	}

}
