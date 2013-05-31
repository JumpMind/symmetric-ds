package org.jumpmind.symmetric.web.rest.model;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "enginelist")
public class EngineList {

    List<Engine> engines;

    public EngineList(Engine... engines) {
        this.setEngines(engines);
    }

    public EngineList() {
        this.engines = new ArrayList<Engine>();
    }

    public void setEngines(Engine[] engines) {
        this.engines = new ArrayList<Engine>();
        for (Engine engine : engines) {
            this.engines.add(engine);
        }
    }

    public Engine[] getEngines() {
        return engines.toArray(new Engine[engines.size()]);
    }
    
    public void addEngine(Engine engine) {
        this.engines.add(engine);
    }
}
