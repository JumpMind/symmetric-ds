package org.jumpmind.symmetric.service.jmx;

import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedOperationParameters;
import org.springframework.jmx.export.annotation.ManagedResource;

@ManagedResource(objectName = "bean:name=Symmetric", description = "An API into symmetric")
public class SymmetricManagedBean {
    @ManagedOperation(description = "Add two numbers")
    @ManagedOperationParameters( {
            @ManagedOperationParameter(name = "x", description = "The first number"),
            @ManagedOperationParameter(name = "y", description = "The second number") })
    public int add(int x, int y) {
        return x + y;
    }
}
