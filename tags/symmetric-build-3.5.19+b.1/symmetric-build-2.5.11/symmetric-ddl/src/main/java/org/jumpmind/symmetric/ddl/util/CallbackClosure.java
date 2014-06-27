package org.jumpmind.symmetric.ddl.util;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.commons.collections.Closure;
import org.jumpmind.symmetric.ddl.DdlUtilsException;

/**
 * A closure that determines a callback for the type of the object and calls it.
 * Note that inheritance is also taken into account. I.e. if the object is of
 * type B which is a subtype of A, and there is only a callback for type A,
 * then this one will be invoked. If there is however also a callback for type B,
 * then only this callback for type B will be invoked and not the one for type A. 
 * 
 * @version $Revision: $
 */
public class CallbackClosure implements Closure
{
    /** The object on which the callbacks will be invoked. */
    private Object _callee;
    /** The parameter types. */
    private Class[] _parameterTypes;
    /** The parameters. */
    private Object[] _parameters;
    /** The position of the callback parameter type. */
    private int _callbackTypePos = -1;
    /** The cached callbacks. */
    private Map _callbacks = new HashMap();

    /**
     * Creates a new closure object.
     * 
     * @param callee         The object on which the callbacks will be invoked
     * @param callbackName   The name of the callback method
     * @param parameterTypes The parameter types. This array has to contain one <code>null</code>
     *                       for the type of the object for which the callback is invoked.
     *                       <code>null</code> or an empty array is regarded to be the
     *                       same as an array containing a single <code>null</code>
     * @param parameters     The actual arguments. The value at the placeholder position
     *                       will be ignored. Can be <code>null</code> if no parameter types
     *                       where given
     */
    public CallbackClosure(Object callee, String callbackName, Class[] parameterTypes, Object[] parameters)
    {
        _callee = callee;

        if ((parameterTypes == null) || (parameterTypes.length == 0))
        {
            _parameterTypes  = new Class[] { null };
            _parameters      = new Object[] { null };
            _callbackTypePos = 0;
        }
        else
        {
            _parameterTypes = new Class[parameterTypes.length];
            _parameters     = new Object[parameterTypes.length];

            for (int idx = 0; idx < parameterTypes.length; idx++)
            {
                if (parameterTypes[idx] == null)
                {
                    if (_callbackTypePos >= 0)
                    {
                        throw new IllegalArgumentException("The parameter types may contain null only once");
                    }
                    _callbackTypePos = idx;
                }
                else
                {
                    _parameterTypes[idx] = parameterTypes[idx];
                    _parameters[idx]     = parameters[idx];
                }
            }
            if (_callbackTypePos < 0)
            {
                throw new IllegalArgumentException("The parameter types need to a null placeholder");
            }
        }
        
        Class type = callee.getClass();

        // we're caching the callbacks
        do
        {
            Method[] methods = type.getDeclaredMethods();

            if (methods != null)
            {
                for (int idx = 0; idx < methods.length; idx++)
                {
                    Method  method     = methods[idx];
                    Class[] paramTypes = methods[idx].getParameterTypes();

                    method.setAccessible(true);
                    if (method.getName().equals(callbackName) && typesMatch(paramTypes))
                    {
                        if (_callbacks.get(paramTypes[_callbackTypePos]) == null)
                        {
                            _callbacks.put(paramTypes[_callbackTypePos], methods[idx]);
                        }
                    }
                }
            }
            type = type.getSuperclass();
        }
        while ((type != null) && !type.equals(Object.class));
    }

    /**
     * Checks whether the given method parameter types match the expected ones.
     * 
     * @param methodParamTypes The method parameter types
     * @return <code>true</code> if the parameter types match
     */
    private boolean typesMatch(Class[] methodParamTypes)
    {
        if ((methodParamTypes == null) || (_parameterTypes.length != methodParamTypes.length))
        {
            return false;
        }
        for (int idx = 0; idx < _parameterTypes.length; idx++)
        {
            if ((idx != _callbackTypePos) && !_parameterTypes[idx].equals(methodParamTypes[idx]))
            {
                return false;
            }
        }
        return true;
    }
    
    /**
     * {@inheritDoc}
     */
    public void execute(Object obj) throws DdlUtilsException
    {
        LinkedList queue = new LinkedList();

        queue.add(obj.getClass());
        while (!queue.isEmpty())
        {
            Class  type     = (Class)queue.removeFirst();
            Method callback = (Method)_callbacks.get(type);

            if (callback != null)
            {
                try
                {
                    _parameters[_callbackTypePos] = obj;
                    callback.invoke(_callee, _parameters);
                    return;
                }
                catch (InvocationTargetException ex)
                {
                    throw new DdlUtilsException(ex.getTargetException());
                }
                catch (IllegalAccessException ex)
                {
                    throw new DdlUtilsException(ex);
                }
            }
            if ((type.getSuperclass() != null) && !type.getSuperclass().equals(Object.class))
            {
                queue.add(type.getSuperclass());
            }

            Class[] baseInterfaces = type.getInterfaces();

            if (baseInterfaces != null)
            {
                for (int idx = 0; idx < baseInterfaces.length; idx++)
                {
                    queue.add(baseInterfaces[idx]);
                }
            }
        }
    }
}
