package org.jumpmind.symmetric.web.rest.model;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "enginelist")
public class EngineList {

    Engine[] engines;

    public EngineList(Engine... engines) {
        this.engines = engines;
    }

    public EngineList() {

    }

    public void setEngines(Engine[] engines) {
        this.engines = engines;
    }

    public Engine[] getEngines() {
        return engines;
    }
}
