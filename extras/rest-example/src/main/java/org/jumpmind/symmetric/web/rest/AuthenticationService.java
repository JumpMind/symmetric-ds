package org.jumpmind.symmetric.web.rest;

import java.lang.annotation.Annotation;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.MDC;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.web.SymmetricEngineHolder;
import org.jumpmind.symmetric.web.WebConstants;
import org.jumpmind.symmetric.web.rest.model.RestError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

@Controller
public class AuthenticationService {
    protected final Logger log = LoggerFactory.getLogger(getClass());
    
    @Autowired
    ServletContext context;
    
    public AuthenticationService() {
        log.warn("Authentication Service created ... ");
    }
    
    @RequestMapping(value = "engine/{engine}/registration-auth", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final RegistrationInfo registrationAuth(@PathVariable("engine") String engineName,
            @RequestParam(value = "user") String user,
            @RequestParam(value = "password") String password) {

        ISymmetricEngine engine = getSymmetricEngine(engineName);
        
        String nodeGroupId = authorize(user, password);
        String externalId = user;
        
        IRegistrationService registrationService = engine.getRegistrationService();
        
        registrationService.openRegistration(nodeGroupId, externalId);
        
        return new RegistrationInfo(nodeGroupId, externalId);
    }
    
    boolean fail = false;
    
    protected String authorize(String user, String password) {
        fail = !fail;
        if (fail) {          
            throw new NotAllowedException(user + " could not be found in active directory");
        } 
        // TODO use LDAP to figure out node group id
        return "region1";
    }
    
    @ExceptionHandler(Exception.class)
    @ResponseBody
    public RestError handleError(Exception ex, HttpServletRequest req) {
        int httpErrorCode = 500;
        Annotation annotation = ex.getClass().getAnnotation(ResponseStatus.class);
        if (annotation != null) {
            httpErrorCode = ((ResponseStatus) annotation).value().value();
        }
        return new RestError(ex, httpErrorCode);
    }
    
    
    protected ISymmetricEngine getSymmetricEngine(String engineName) {
        SymmetricEngineHolder holder = getSymmetricEngineHolder();

        ISymmetricEngine engine = null;
        if (StringUtils.isNotBlank(engineName)) {
            engine = holder.getEngines().get(engineName);
        }
        if (engine == null) {
            throw new NotFoundException();
        } else if (!engine.getParameterService().is(ParameterConstants.REST_API_ENABLED)) {
            throw new NotAllowedException("The REST API was not enabled for %s",
                    engine.getEngineName());
        } else {
            MDC.put("engineName", engine.getEngineName());
            return engine;
        }
    }
    
    protected SymmetricEngineHolder getSymmetricEngineHolder() {
        SymmetricEngineHolder holder = (SymmetricEngineHolder) context
                .getAttribute(WebConstants.ATTR_ENGINE_HOLDER);
        if (holder == null) {
            throw new NotFoundException();
        }
        return holder;
    }




}
