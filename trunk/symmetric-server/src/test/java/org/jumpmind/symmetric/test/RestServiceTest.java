package org.jumpmind.symmetric.test;

import java.sql.Types;
import java.util.List;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.web.rest.NotAllowedException;
import org.jumpmind.symmetric.web.rest.RestService;
import org.jumpmind.symmetric.web.rest.model.PullDataResults;
import org.jumpmind.symmetric.web.rest.model.RegistrationInfo;
import org.junit.Assert;

public class RestServiceTest extends AbstractTest {

    @Override
    protected Table[] getTables(String name) {
        Table a = new Table("a");
        a.addColumn(new Column("id", true, Types.INTEGER, -1, -1));
        a.addColumn(new Column("notes", false, Types.VARCHAR, 255, -1));
        return new Table[] { a };
    }

    @Override
    protected String[] getGroupNames() {
        return new String[] { "server", "client" };
    }

    @Override
    protected void test(ISymmetricEngine rootServer, ISymmetricEngine clientServer)
            throws Exception {

        loadConfigAtRegistrationServer();

        RestService restService = getRegServer().getRestService();
        ISymmetricEngine engine = getRegServer().getEngine();
        IParameterService parameterService = engine.getParameterService();
        INodeService nodeService = engine.getNodeService();

        parameterService.saveParameter(ParameterConstants.REST_API_ENABLED, Boolean.TRUE,
                "unit_test");

        Assert.assertNotNull("Could not find the rest service in the application context",
                restService);

        List<Node> nodes = nodeService.findAllNodes();

        Assert.assertEquals("Expected there to only be one node registered", 1, nodes.size());
        Assert.assertEquals("The only node we expected to be registered is a server node",
                "server", nodes.get(0).getNodeGroupId());

        RegistrationInfo registrationInfo = restService.registerNode("client", "client",
                DatabaseNamesConstants.SQLITE, "3.0");

        Assert.assertNotNull("Registration should have returned a result object", registrationInfo);
        Assert.assertFalse("Registration should not have been open",
                registrationInfo.isRegistered());
        Assert.assertEquals("Expected there to only be one node registered", 1, nodes.size());

        engine.openRegistration("client", "client");

        registrationInfo = restService.registerNode("client", "client",
                DatabaseNamesConstants.SQLITE, "3.0");

        Assert.assertNotNull("Registration should have returned a result object", registrationInfo);
        Assert.assertTrue("Registration should have been open", registrationInfo.isRegistered());
        Assert.assertEquals("client", registrationInfo.getNodeId());

        try {
            restService.pullData(registrationInfo.getNodeId(), "wrong password");
            Assert.fail("We should have received an exception");
        } catch (NotAllowedException ex) {
        }

        PullDataResults results = restService.pullData("server", registrationInfo.getNodeId(),
                registrationInfo.getNodePassword());
        Assert.assertNotNull("Should have a non null results object", results);
        Assert.assertEquals(0, results.getNbrBatches());
        
        engine.getSqlTemplate().update("insert into a values(?, ?)", 1, "this is a test");
        
        engine.route();
        
        results = restService.pullData("server", registrationInfo.getNodeId(),
                registrationInfo.getNodePassword());
        Assert.assertNotNull("Should have a non null results object", results);
        Assert.assertEquals(1, results.getNbrBatches());

    }

}
