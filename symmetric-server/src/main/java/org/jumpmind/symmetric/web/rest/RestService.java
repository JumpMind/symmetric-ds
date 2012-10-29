package org.jumpmind.symmetric.web.rest;

import java.util.Set;

import javax.servlet.ServletContext;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.web.SymmetricEngineHolder;
import org.jumpmind.symmetric.web.WebConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class RestService {

    @Autowired
    ServletContext context;

    @RequestMapping(value = "/{engine}/identity", method = RequestMethod.GET)
    @ResponseBody
    public final String identity(@PathVariable("engine") String engineName) {
        ISymmetricEngine engine = getSymmetricEngine(engineName);
        return engine.getNodeService().findIdentityNodeId();
    }

    @RequestMapping(value = "/engines", method = RequestMethod.GET)
    @ResponseBody
    public final Set<String> engines() {
        return getSymmetricEngineHolder().getEngines().keySet();
    }

    protected SymmetricEngineHolder getSymmetricEngineHolder() {
        SymmetricEngineHolder holder = (SymmetricEngineHolder) context
                .getAttribute(WebConstants.ATTR_ENGINE_HOLDER);
        if (holder == null) {
            throw new NotFoundException();
        }
        return holder;
    }

    protected ISymmetricEngine getSymmetricEngine(String engineName) {
        SymmetricEngineHolder holder = getSymmetricEngineHolder();
        ISymmetricEngine engine = holder.getEngines().get(engineName);
        if (engine == null) {
            throw new NotFoundException();
        } else {
            return engine;
        }
    }

}
