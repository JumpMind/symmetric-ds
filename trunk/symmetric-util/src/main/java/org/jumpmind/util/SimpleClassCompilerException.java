package org.jumpmind.util;

import java.util.List;

import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;

public class SimpleClassCompilerException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    
    List<Diagnostic<? extends JavaFileObject>> diagnostics;
    
    public SimpleClassCompilerException(List<Diagnostic<? extends JavaFileObject>> diagnostics) {
        this.diagnostics = diagnostics;
    }
    
    public List<Diagnostic<? extends JavaFileObject>> getDiagnostics() {
        return diagnostics;
    }
    
}
