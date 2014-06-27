/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.jumpmind.symmetric.test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Assert;
import org.junit.internal.runners.ClassRoadie;
import org.junit.internal.runners.CompositeRunner;
import org.junit.internal.runners.InitializationError;
import org.junit.internal.runners.JUnit4ClassRunner;
import org.junit.internal.runners.MethodValidator;
import org.junit.internal.runners.TestClass;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Merge JUnits Parameterized runner and their Suite runner so we have a nice
 * efficient way to run an entire suite of tests against parameterized 'sets' of
 * data across the entire suite.
 */
public class ParameterizedSuite extends CompositeRunner {

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public static @interface ParameterMatcher {
        String[] value();
    }
    
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public static @interface ParameterExcluder {
        String[] value();
    }

    static class TestClassRunnerForParameters extends JUnit4ClassRunner {
        private final Object[] fParameters;

        private final Constructor<?> fConstructor;

        private List<Method> methods;

        TestClassRunnerForParameters(TestClass testClass, Object[] parameters) throws InitializationError {
            super(testClass.getJavaClass());
            fParameters = parameters;
            fConstructor = getOnlyConstructor();
            filterParameters();
        }

        protected void filterParameters() {
            for (Iterator<Method> iterator = methods.iterator(); iterator.hasNext();) {
                Method method = (Method) iterator.next();
                ParameterMatcher match = method.getAnnotation(ParameterMatcher.class);
                if (match != null) {
                    boolean remove = true;
                    for (Object p : fParameters) {
                        String[] matchValues = match.value();
                        for (String matchValue : matchValues) {
                            if (p != null && p.toString().equals(matchValue)) {
                                remove = false;
                            }                            
                        }
                    }
                    if (remove) {
                        iterator.remove();
                    }
                }
                
                ParameterExcluder excluder = method.getAnnotation(ParameterExcluder.class);
                if (excluder != null) {
                    boolean remove = false;
                    for (Object p : fParameters) {
                        String[] excludeValues = excluder.value();
                        for (String excludeValue : excludeValues) {
                            if (p != null && p.toString().equals(excludeValue)) {
                                remove = true;
                            }                            
                        }
                    }
                    if (remove) {
                        iterator.remove();
                    }
                }
            }
        }

        @Override
        protected Object createTest() throws Exception {
            return fConstructor.newInstance(fParameters);
        }

        @Override
        protected String getName() {
            return String.format("%s with params %s", getTestClass().getName(), ArrayUtils.toString(fParameters));
        }

        /**
         * Get a sneaky handle on methods so we can filter them.
         */
        @Override
        protected List<Method> getTestMethods() {
            methods = super.getTestMethods();
            return methods;
        }

        @Override
        protected String testName(final Method method) {
            return String.format("%s with params %s", method.getName(), ArrayUtils.toString(fParameters));
        }

        private Constructor<?> getOnlyConstructor() {
            Constructor<?> c = null;
            Constructor<?>[] constructors = getTestClass().getJavaClass().getConstructors();
            for (Constructor<?> constructor : constructors) {
                if (constructor.getGenericParameterTypes().length == fParameters.length) {
                    c = constructor;
                    break;
                }
            }
            Assert.assertNotNull("Could not find an appropriate constructor for " + getName(), c);
            return c;
        }

        @Override
        protected void validate() throws InitializationError {
            // do nothing: validated before.
        }

        @Override
        public void run(RunNotifier notifier) {
            runMethods(notifier);
        }
    }

    public ParameterizedSuite(Class<?> klass) throws Exception {
        this(klass, getAnnotatedClasses(klass));
    }

    // This won't work correctly in the face of concurrency. For that we need to
    // add parameters to getRunner(), which would be much more complicated.
    private static Set<Class<?>> parents = new HashSet<Class<?>>();
    private TestClass fTestClass;

    protected ParameterizedSuite(Class<?> klass, Class<?>[] annotatedClasses) throws Exception {
        super(klass.getName());

        fTestClass = new TestClass(klass);

        addParent(klass);
        for (final Object each : getParametersList()) {
            if (each instanceof Object[]) {
                for (Class<?> clazz : annotatedClasses) {
                    add(new TestClassRunnerForParameters(new TestClass(clazz), (Object[]) each));
                }
            } else {
                throw new Exception(String.format("%s.%s() must return a Collection of arrays.", fTestClass.getName(),
                        getParametersMethod().getName()));
            }
        }
        removeParent(klass);
        MethodValidator methodValidator = new MethodValidator(fTestClass);
        methodValidator.validateStaticMethods();
        methodValidator.assertValid();
    }

    private Class<?> addParent(Class<?> parent) throws InitializationError {
        if (!parents.add(parent))
            throw new InitializationError(String.format(
                    "class '%s' (possibly indirectly) contains itself as a SuiteClass", parent.getName()));
        return parent;
    }

    private void removeParent(Class<?> klass) {
        parents.remove(klass);
    }

    private static Class<?>[] getAnnotatedClasses(Class<?> klass) throws InitializationError {
        SuiteClasses annotation = klass.getAnnotation(SuiteClasses.class);
        if (annotation == null)
            throw new InitializationError(String.format("class '%s' must have a SuiteClasses annotation", klass
                    .getName()));
        Class<?>[] classes = new Class[annotation.value().length + 1];
        classes[0] = klass;
        for (int i = 1; i <= annotation.value().length; i++) {
            classes[i] = annotation.value()[i - 1];
        }
        return classes;
    }

    protected void validate(MethodValidator methodValidator) {
        methodValidator.validateStaticMethods();
        methodValidator.validateInstanceMethods();
    }

    private Collection<?> getParametersList() throws IllegalAccessException, InvocationTargetException, Exception {
        return (Collection<?>) getParametersMethod().invoke(null);
    }

    private Method getParametersMethod() throws Exception {
        List<Method> methods = fTestClass.getAnnotatedMethods(Parameters.class);
        for (Method each : methods) {
            int modifiers = each.getModifiers();
            if (Modifier.isStatic(modifiers) && Modifier.isPublic(modifiers))
                return each;
        }

        throw new Exception("No public static parameters method on class " + getName());
    }

    @Override
    public void run(final RunNotifier notifier) {
        new ClassRoadie(notifier, fTestClass, getDescription(), new Runnable() {
            public void run() {
                runChildren(notifier);
            }
        }).runProtected();
    }

}
