package org.jumpmind.symmetric.core.service;

import java.util.List;

import org.jumpmind.symmetric.core.IEnvironment;
import org.jumpmind.symmetric.core.SymmetricTables;
import org.jumpmind.symmetric.core.db.Query;
import org.jumpmind.symmetric.core.db.mapper.NodeMapper;
import org.jumpmind.symmetric.core.model.Node;


public class NodeService extends AbstractParameterizedService {
    
    private final static NodeMapper NODE_MAPPER = new NodeMapper();
    
    private Node cachedNodeIdentity;
    
    public NodeService(IEnvironment environment, ParameterService parameterService) {
        super(environment, parameterService);
    }
    
    public Node findIdentity() {
        return findIdentity(true);
    }

    public Node findIdentity(boolean useCache) {
        if (cachedNodeIdentity == null || useCache == false) {
            List<Node> list = dbDialect.getSqlTemplate().query(new Query(0, tables.getSymmetricTable(SymmetricTables.NODE), tables.getSymmetricTable(SymmetricTables.NODE_IDENTITY)),
                    NODE_MAPPER);
            cachedNodeIdentity = list.size() > 0 ? list.get(0) : null;
        }
        return cachedNodeIdentity;
    }
    
}
