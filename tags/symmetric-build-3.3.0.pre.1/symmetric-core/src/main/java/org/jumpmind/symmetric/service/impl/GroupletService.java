/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License. 
 */
package org.jumpmind.symmetric.service.impl;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Grouplet;
import org.jumpmind.symmetric.model.Grouplet.GroupletLinkPolicy;
import org.jumpmind.symmetric.model.GroupletLink;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.model.TriggerRouterGrouplet;
import org.jumpmind.symmetric.model.TriggerRouterGrouplet.AppliesWhen;
import org.jumpmind.symmetric.service.IGroupletService;

public class GroupletService extends AbstractService implements IGroupletService {

    protected ISymmetricEngine engine;

    protected List<Grouplet> cache;

    protected long lastCacheTime = 0;

    public GroupletService(ISymmetricEngine engine) {
        super(engine.getParameterService(), engine.getSymmetricDialect());
        this.engine = engine;

        setSqlMap(new GroupletServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
    }
    
    public void reloadGrouplets() {
        lastCacheTime = 0;
    }

    public boolean isSourceEnabled(TriggerRouter triggerRouter) {
        boolean enabled = true;
        Node node = engine.getNodeService().findIdentity();
        if (node == null) {
            enabled = false;
        } else {
            List<Grouplet> grouplets = getGroupletsFor(triggerRouter, AppliesWhen.S, false);
            if (grouplets != null && grouplets.size() > 0) {
                enabled = false;
                for (Grouplet grouplet : grouplets) {
                    GroupletLinkPolicy policy = grouplet.getGroupletLinkPolicy();
                    List<GroupletLink> links = grouplet.getGroupletLinks();
                    boolean foundMatch = false;
                    for (GroupletLink groupletLink : links) {
                        if (groupletLink.getExternalId().equals(node.getExternalId())) {
                            foundMatch = true;
                        }
                    }

                    if ((foundMatch && policy == GroupletLinkPolicy.I)
                            || (!foundMatch && policy == GroupletLinkPolicy.E)) {
                        enabled = true;
                    }
                }
            }
        }
        return enabled;
    }
    
    public boolean isTargetEnabled(TriggerRouter triggerRouter, Node node) {
        Set<Node> nodes = new HashSet<Node>(1);
        nodes.add(node);
        return getTargetEnabled(triggerRouter, nodes).size() > 0;
    }

    public Set<Node> getTargetEnabled(TriggerRouter triggerRouter, Set<Node> nodes) {
        Set<Node> matchedNodes = new HashSet<Node>();
        Set<Node> excludedNodes = new HashSet<Node>();
        List<Grouplet> grouplets = getGroupletsFor(triggerRouter, AppliesWhen.T, false);
        if (grouplets != null && grouplets.size() > 0) {
            for (Grouplet grouplet : grouplets) {
                GroupletLinkPolicy policy = grouplet.getGroupletLinkPolicy();
                List<GroupletLink> links = grouplet.getGroupletLinks();
                for (GroupletLink groupletLink : links) {
                    for (Node node : nodes) {
                        if (groupletLink.getExternalId().equals(node.getExternalId())) {
                            if (policy == GroupletLinkPolicy.I) {
                                matchedNodes.add(node);
                            } else {
                                excludedNodes.add(node);
                            }
                        }
                    }
                }
            }

            Set<Node> toReturn = new HashSet<Node>();

            excludedNodes.removeAll(matchedNodes);

            if (excludedNodes.size() > 0) {
                toReturn.addAll(nodes);
                toReturn.removeAll(excludedNodes);
            } else {
                toReturn.addAll(matchedNodes);
            }

            return toReturn;
        } else {
            return nodes;
        }

    }

    public List<Grouplet> getGrouplets(boolean refreshCache) {
        long maxCacheTime = parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_GROUPLETS_IN_MS);
        List<Grouplet> all = cache;
        if (all == null || System.currentTimeMillis() - lastCacheTime >= maxCacheTime
                || lastCacheTime == 0 || refreshCache) {
            ISqlTemplate sqlTemplate = platform.getSqlTemplate();
            final Map<String, Grouplet> groupletMap = new HashMap<String, Grouplet>();
            all = sqlTemplate.query(getSql("selectGroupletSql"), new ISqlRowMapper<Grouplet>() {
                public Grouplet mapRow(Row rs) {
                    Grouplet grouplet = new Grouplet();
                    grouplet.setGroupletId(rs.getString("grouplet_id"));
                    grouplet.setDescription(rs.getString("description"));
                    grouplet.setGroupletLinkPolicy(GroupletLinkPolicy.valueOf(rs
                            .getString("grouplet_link_policy")));
                    grouplet.setCreateTime(rs.getDateTime("create_time"));
                    grouplet.setLastUpdateBy(rs.getString("last_update_by"));
                    grouplet.setLastUpdateTime(rs.getDateTime("last_update_time"));
                    groupletMap.put(grouplet.getGroupletId(), grouplet);
                    return grouplet;
                }
            });

            sqlTemplate.query(getSql("selectGroupletLinkSql"), new ISqlRowMapper<GroupletLink>() {
                public GroupletLink mapRow(Row rs) {
                    GroupletLink groupletLink = new GroupletLink();
                    String groupletId = rs.getString("grouplet_id");
                    Grouplet grouplet = groupletMap.get(groupletId);
                    groupletLink.setExternalId(rs.getString("external_id"));
                    groupletLink.setCreateTime(rs.getDateTime("create_time"));
                    groupletLink.setLastUpdateBy(rs.getString("last_update_by"));
                    groupletLink.setLastUpdateTime(rs.getDateTime("last_update_time"));

                    if (grouplet != null) {
                        grouplet.getGroupletLinks().add(groupletLink);
                    }
                    return groupletLink;
                }
            });

            sqlTemplate.query(getSql("selectTriggerRouterGroupletSql"),
                    new ISqlRowMapper<TriggerRouterGrouplet>() {
                        public TriggerRouterGrouplet mapRow(Row rs) {
                            TriggerRouterGrouplet trGrouplet = new TriggerRouterGrouplet();
                            String groupletId = rs.getString("grouplet_id");
                            Grouplet grouplet = groupletMap.get(groupletId);
                            trGrouplet.setAppliesWhen(AppliesWhen.valueOf(rs
                                    .getString("applies_when")));
                            trGrouplet.setRouterId(rs.getString("router_id"));
                            trGrouplet.setTriggerId(rs.getString("trigger_id"));
                            trGrouplet.setCreateTime(rs.getDateTime("create_time"));
                            trGrouplet.setLastUpdateBy(rs.getString("last_update_by"));
                            trGrouplet.setLastUpdateTime(rs.getDateTime("last_update_time"));

                            if (grouplet != null) {
                                grouplet.getTriggerRouterGrouplets().add(trGrouplet);
                            }
                            return trGrouplet;
                        }
                    });

            cache = all;
            lastCacheTime = System.currentTimeMillis();
        }
        return all;
    }

    protected List<Grouplet> getGroupletsFor(TriggerRouter triggerRouter, AppliesWhen appliesWhen,
            boolean refreshCache) {
        List<Grouplet> all = getGrouplets(refreshCache);
        List<Grouplet> grouplets = new ArrayList<Grouplet>();
        for (Grouplet grouplet : all) {
            List<TriggerRouterGrouplet> trGrouplets = grouplet.getTriggerRouterGrouplets();
            for (TriggerRouterGrouplet trGrouplet : trGrouplets) {
                if (trGrouplet.getTriggerId().equals(triggerRouter.getTrigger().getTriggerId())
                        && trGrouplet.getRouterId().equals(triggerRouter.getRouter().getRouterId())
                        && (trGrouplet.getAppliesWhen() == appliesWhen || trGrouplet.getAppliesWhen() == AppliesWhen.B)) {
                    grouplets.add(grouplet);
                }
            }
        }
        return grouplets;
    }

    public void saveGrouplet(Grouplet grouplet) {
        ISqlTemplate sqlTemplate = platform.getSqlTemplate();
        grouplet.setLastUpdateTime(new Date());
        if (sqlTemplate.update(
                getSql("updateGroupletSql"),
                new Object[] { grouplet.getGroupletLinkPolicy().name(), grouplet.getDescription(),
                        grouplet.getCreateTime(), grouplet.getLastUpdateBy(),
                        grouplet.getLastUpdateTime(), grouplet.getGroupletId() }, new int[] {
                        Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR,
                        Types.TIMESTAMP, Types.VARCHAR }) == 0) {
            grouplet.setCreateTime(new Date());
            sqlTemplate.update(
                    getSql("insertGroupletSql"),
                    new Object[] { grouplet.getGroupletLinkPolicy().name(),
                            grouplet.getDescription(), grouplet.getCreateTime(),
                            grouplet.getLastUpdateBy(), grouplet.getLastUpdateTime(),
                            grouplet.getGroupletId() }, new int[] { Types.VARCHAR, Types.VARCHAR,
                            Types.TIMESTAMP, Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR });

        }
    }

    public void deleteGrouplet(Grouplet grouplet) {
        List<GroupletLink> links = grouplet.getGroupletLinks();
        for (GroupletLink link : links) {
            deleteGroupletLink(grouplet, link);
        }

        List<TriggerRouterGrouplet> triggerRouters = grouplet.getTriggerRouterGrouplets();
        for (TriggerRouterGrouplet triggerRouterGrouplet : triggerRouters) {
            deleteTriggerRouterGrouplet(grouplet, triggerRouterGrouplet);
        }

        ISqlTemplate sqlTemplate = platform.getSqlTemplate();
        sqlTemplate.update(getSql("deleteGroupletSql"), new Object[] { grouplet.getGroupletId() },
                new int[] { Types.VARCHAR });

    }

    public void saveGroupletLink(Grouplet grouplet, GroupletLink link) {
        ISqlTemplate sqlTemplate = platform.getSqlTemplate();
        link.setLastUpdateTime(new Date());
        if (sqlTemplate.update(
                getSql("updateGroupletLinkSql"),
                new Object[] { link.getCreateTime(), link.getLastUpdateBy(),
                        link.getLastUpdateTime(), grouplet.getGroupletId(), link.getExternalId() },
                new int[] { Types.TIMESTAMP, Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR,
                        Types.VARCHAR }) == 0) {
            link.setCreateTime(new Date());
            sqlTemplate.update(getSql("insertGroupletLinkSql"), new Object[] {
                    link.getCreateTime(), link.getLastUpdateBy(), link.getLastUpdateTime(),
                    grouplet.getGroupletId(), link.getExternalId() }, new int[] { Types.TIMESTAMP,
                    Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR });

        }
    }

    public void deleteGroupletLink(Grouplet grouplet, GroupletLink link) {
        ISqlTemplate sqlTemplate = platform.getSqlTemplate();
        sqlTemplate.update(getSql("deleteGroupletLinkSql"), new Object[] {
                grouplet.getGroupletId(), link.getExternalId() }, new int[] { Types.VARCHAR,
                Types.VARCHAR });
    }

    public void saveTriggerRouterGrouplet(Grouplet grouplet,
            TriggerRouterGrouplet triggerRouterGrouplet) {
        ISqlTemplate sqlTemplate = platform.getSqlTemplate();
        triggerRouterGrouplet.setLastUpdateTime(new Date());
        if (sqlTemplate.update(getSql("updateTriggerRouterGroupletSql"), new Object[] {
                triggerRouterGrouplet.getCreateTime(), triggerRouterGrouplet.getLastUpdateBy(),
                triggerRouterGrouplet.getLastUpdateTime(), grouplet.getGroupletId(),
                triggerRouterGrouplet.getAppliesWhen().name(),
                triggerRouterGrouplet.getTriggerId(), triggerRouterGrouplet.getRouterId() },
                new int[] { Types.TIMESTAMP, Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR }) == 0) {
            triggerRouterGrouplet.setCreateTime(new Date());
            sqlTemplate.update(
                    getSql("insertTriggerRouterGroupletSql"),
                    new Object[] { triggerRouterGrouplet.getCreateTime(),
                            triggerRouterGrouplet.getLastUpdateBy(),
                            triggerRouterGrouplet.getLastUpdateTime(), grouplet.getGroupletId(),
                            triggerRouterGrouplet.getAppliesWhen().name(),
                            triggerRouterGrouplet.getTriggerId(),
                            triggerRouterGrouplet.getRouterId() }, new int[] { Types.TIMESTAMP,
                            Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR,
                            Types.VARCHAR, Types.VARCHAR });

        }
    }

    public void deleteTriggerRouterGrouplet(Grouplet grouplet,
            TriggerRouterGrouplet triggerRouterGrouplet) {
        ISqlTemplate sqlTemplate = platform.getSqlTemplate();
        sqlTemplate
                .update(getSql("deleteTriggerRouterGroupletSql"),
                        new Object[] { grouplet.getGroupletId(),
                                triggerRouterGrouplet.getAppliesWhen().name(),
                                triggerRouterGrouplet.getTriggerId(),
                                triggerRouterGrouplet.getRouterId() }, new int[] { Types.VARCHAR,
                                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR });
    }

}
