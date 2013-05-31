package org.jumpmind.symmetric.web.rest.model;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Engine {

    private String name;

    public Engine(String name) {
        setName(name);
    }

    public Engine() {

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
