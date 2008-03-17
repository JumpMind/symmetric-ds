package org.jumpmind.symmetric.web;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.DataEventAction;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockFilterConfig;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockServletContext;
import org.springframework.web.context.ConfigurableWebApplicationContext;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.XmlWebApplicationContext;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SymmetricFilterTest {

	private static final String GOOD_SECURITY_TOKEN = "1";
	private static final String GOOD_NODE_ID = "1";
	private static final String BAD_SECURITY_TOKEN = "2";
	private static final String BAD_NODE_ID = "2";

	private final class MockRegistrationService implements IRegistrationService {
		public boolean isAutoRegistration() {
			return true;
		}

		public void openRegistration(String nodeGroupId, String externalId) {

		}

		public void reOpenRegistration(String nodeId) {

		}

		public boolean registerNode(Node node, OutputStream out)
				throws IOException {
			return false;
		}
	}

	private final class MockNodeService implements INodeService {
		public Node findIdentity() {
			return null;
		}

		public Node findNode(String nodeId) {
			return null;
		}

		public Node findNodeByExternalId(String nodeGroupId, String externalId) {
			return null;
		}

		public NodeSecurity findNodeSecurity(String nodeId) {
			return null;
		}

		public List<Node> findNodesToPull() {
			return null;
		}

		public List<Node> findNodesToPushTo() {
			return null;
		}

		public List<Node> findSourceNodesFor(DataEventAction eventAction) {
			return null;
		}

		public List<Node> findTargetNodesFor(DataEventAction eventAction) {
			return null;
		}

		public void ignoreNodeChannelForExternalId(boolean ignore,
				String channelId, String nodeGroupId, String externalId) {

		}

		public boolean isExternalIdRegistered(String nodeGroupId,
				String externalId) {
			return false;
		}

		public boolean isNodeAuthorized(String nodeId, String password) {
			return GOOD_NODE_ID.equals(nodeId) && GOOD_SECURITY_TOKEN.equals(password);
		}

		public boolean isRegistrationEnabled(String nodeId) {
			return false;
		}

		public boolean setInitialLoadEnabled(String nodeId,
				boolean initialLoadEnabled) {
			return false;
		}

		public boolean updateNode(Node node) {
			return false;
		}

		public boolean updateNodeSecurity(NodeSecurity security) {
			return false;
		}
	}

	protected ConfigurableWebApplicationContext applicationContext;
	protected ServletContext servletContext;

	@BeforeMethod(alwaysRun = true)
	protected void springTestContextBeforeTestMethod(Method method)
			throws Exception {
		servletContext = new MockServletContext();
		applicationContext = new XmlWebApplicationContext();
		applicationContext.setServletContext(servletContext);
		applicationContext.setConfigLocation("/symmetric-web.xml");
		applicationContext.refresh();
		servletContext.setAttribute(
				WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE,
				applicationContext);
		// if ("testAuthenticationFilterRegistrationRequired".equals(method
		// .getName())) {
		{
			applicationContext.getBeanFactory().registerSingleton(
					Constants.NODE_SERVICE, new MockNodeService());
			applicationContext.getBeanFactory().registerSingleton(
					Constants.REGISTRATION_SERVICE,
					new MockRegistrationService());
		}
	}

	@DataProvider(name = "authenticationFilterForbiddenParams")
	public Object[][] authenticationFilterForbiddenParams() {
		final Map<String, String> emptyAuthentication = new HashMap<String, String>();
		emptyAuthentication.put(WebConstants.SECURITY_TOKEN, "");
		emptyAuthentication.put(WebConstants.NODE_ID, "");

		return new Object[][] {
				{ "GET", "/ack", null },
				{ "GET", "/ack/", null },
				{ "GET", "/ack/more", null },
				{ "GET", "/ack?name=value", null },
				{ "GET", "/ack?name=value&name=value", null },
				{
						"GET",
						String.format("/ack?%s=1&%s=2",
								WebConstants.SECURITY_TOKEN,
								WebConstants.NODE_ID), null },
				{ "GET", "/ack", emptyAuthentication },
				{ "PUT", "/ack", null },
				{ "POST", "/ack", null },
				{ "DELETE", "/ack", null },
				{ "TRACE", "/ack", null },
				{ "OPTIONS", "/ack", null },
				{ "HEAD", "/ack", null },
				{ "GET", "/pull", null },
				{ "GET", "/pull/", null },
				{ "GET", "/pull/more", null },
				{ "GET", "/pull?name=value", null },
				{ "GET", "/pull?name=value&name=value", null },
				{
						"GET",
						String.format("/pull?%s=1&%s=2",
								WebConstants.SECURITY_TOKEN,
								WebConstants.NODE_ID), null },
				{ "GET", "/pull", emptyAuthentication },
				{ "PUT", "/pull", null },
				{ "POST", "/pull", null },
				{ "DELETE", "/pull", null },
				{ "TRACE", "/pull", null },
				{ "OPTIONS", "/pull", null },
				{ "HEAD", "/pull", null },
				{ "GET", "/push", null },
				{ "GET", "/push/", null },
				{ "GET", "/push/more", null },
				{ "GET", "/push?name=value", null },
				{ "GET", "/push?name=value&name=value", null },
				{
						"GET",
						String.format("/push?%s=1&%s=2",
								WebConstants.SECURITY_TOKEN,
								WebConstants.NODE_ID), null },
				{ "GET", "/push", emptyAuthentication },
				{ "PUT", "/push", null }, { "POST", "/push", null },
				{ "DELETE", "/push", null }, { "TRACE", "/push", null },
				{ "OPTIONS", "/push", null }, { "HEAD", "/push", null }, };
	}

	@Test(groups = "continuous", dataProvider = "authenticationFilterForbiddenParams")
	public void testAuthenticationFilterForbidden(String method, String uri,
			Map<String, String> parameters) throws Exception {

		final SymmetricFilter filter = new SymmetricFilter();
		filter.init(new MockFilterConfig(servletContext));
		final MockHttpServletRequest request = new MockHttpServletRequest(
				servletContext, method, uri);
		if (parameters != null) {
			request.setParameters(parameters);
		}
		final MockHttpServletResponse response = new MockHttpServletResponse();
		filter.doFilter(request, response, new MockFilterChain());
		Assert.assertEquals(response.getStatus(),
				HttpServletResponse.SC_FORBIDDEN);
		filter.destroy();
	}

	@DataProvider(name = "authenticationFilterRegistrationRequiredParams")
	public Object[][] authenticationFilterRegistrationRequiredParams() {
		final Map<String, String> badAuthentication = new HashMap<String, String>();
		badAuthentication.put(WebConstants.SECURITY_TOKEN, BAD_SECURITY_TOKEN);
		badAuthentication.put(WebConstants.NODE_ID, BAD_NODE_ID);
		return new Object[][] { { "GET", "/ack", badAuthentication },
				{ "GET", "/pull", badAuthentication },
				{ "GET", "/push", badAuthentication }, };
	}

	@Test(groups = "continuous", dataProvider = "authenticationFilterRegistrationRequiredParams")
	public void testAuthenticationFilterRegistrationRequired(String method,
			String uri, Map<String, String> parameters) throws Exception {

		final SymmetricFilter filter = new SymmetricFilter();
		filter.init(new MockFilterConfig(servletContext));

		final MockHttpServletRequest request = new MockHttpServletRequest(
				servletContext, method, uri);
		if (parameters != null) {
			request.setParameters(parameters);
		}
		final MockHttpServletResponse response = new MockHttpServletResponse();
		filter.doFilter(request, response, new MockFilterChain());
		Assert.assertEquals(response.getStatus(),
				WebConstants.REGISTRATION_REQUIRED);
		filter.destroy();
	}
	
	@DataProvider(name = "authenticationFilterParams")
	public Object[][] authenticationFilterParams() {
		final Map<String, String> goodAuthentication = new HashMap<String, String>();
		goodAuthentication.put(WebConstants.SECURITY_TOKEN, GOOD_SECURITY_TOKEN);
		goodAuthentication.put(WebConstants.NODE_ID, GOOD_NODE_ID);
		return new Object[][] { { "GET", "/ack", goodAuthentication },
				{ "GET", "/pull", goodAuthentication },
				{ "GET", "/push", goodAuthentication }, };
	}

	@Test(groups = "continuous", dataProvider = "authenticationFilterParams")
	public void testAuthenticationFilter(String method,
			String uri, Map<String, String> parameters) throws Exception {

		final SymmetricFilter filter = new SymmetricFilter();
		filter.init(new MockFilterConfig(servletContext));

		final MockHttpServletRequest request = new MockHttpServletRequest(
				servletContext, method, uri);
		if (parameters != null) {
			request.setParameters(parameters);
		}
		final MockHttpServletResponse response = new MockHttpServletResponse();
		filter.doFilter(request, response, new MockFilterChain());
		Assert.assertEquals(response.getStatus(),
				HttpServletResponse.SC_OK);
		filter.destroy();
	}
}
