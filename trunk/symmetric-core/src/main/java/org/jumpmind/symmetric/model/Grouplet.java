package org.jumpmind.symmetric.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class Grouplet {

    public enum GroupletLinkPolicy {
        I, E
    };

    protected List<GroupletLink> groupletLinks = new ArrayList<GroupletLink>();
    protected List<TriggerRouterGrouplet> triggerRouterGrouplets = new ArrayList<TriggerRouterGrouplet>();
    protected String groupletId;
    protected String description;
    protected GroupletLinkPolicy groupletLinkPolicy = GroupletLinkPolicy.I;
    protected Date createTime;
    protected Date lastUpdateTime;
    protected String lastUpdateBy;

    public String getGroupletId() {
        return groupletId;
    }

    public void setGroupletId(String groupletId) {
        this.groupletId = groupletId;
    }

    public GroupletLinkPolicy getGroupletLinkPolicy() {
        return groupletLinkPolicy;
    }

    public void setGroupletLinkPolicy(GroupletLinkPolicy groupletLinkPolicy) {
        this.groupletLinkPolicy = groupletLinkPolicy;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = StringUtils.abbreviate(description, 255);
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setLastUpdateBy(String lastUpdateBy) {
        this.lastUpdateBy = lastUpdateBy;
    }

    public String getLastUpdateBy() {
        return lastUpdateBy;
    }

    public void setLastUpdateTime(Date lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setGroupletLinks(List<GroupletLink> groupletLinks) {
        this.groupletLinks = groupletLinks;
    }

    public List<GroupletLink> getGroupletLinks() {
        return groupletLinks;
    }

    public void setTriggerRouterGrouplets(List<TriggerRouterGrouplet> triggerRouterGrouplets) {
        this.triggerRouterGrouplets = triggerRouterGrouplets;
    }

    public List<TriggerRouterGrouplet> getTriggerRouterGrouplets() {
        return triggerRouterGrouplets;
    }
}
