/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */

package org.jumpmind.symmetric.test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.Runner;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.Suite;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.springframework.util.ReflectionUtils;

/**
 * Merge JUnits Parameterized runner and their Suite runner so we have a nice
 * efficient way to run an entire suite of tests against parameterized 'sets' of
 * data across the entire suite.
 *
 * 
 */
public class ParameterizedSuite extends Suite {

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

    static class TestClassRunnerForParameters extends BlockJUnit4ClassRunner {
        private final Object[] fParameters;
        private final Method initMethod;

        TestClassRunnerForParameters(Class<?> type, Object[] parameterList)
                throws org.junit.runners.model.InitializationError {
            super(type);
            fParameters = parameterList;
            initMethod = getInitMethod();
            filterParameters();
        }

        @Override
        protected void validateConstructor(List<Throwable> errors) {
        }

        protected void filterParameters() {
            List<FrameworkMethod> methods = getTestClass().getAnnotatedMethods(Test.class);
            for (Iterator<FrameworkMethod> iterator = methods.iterator(); iterator.hasNext();) {
                FrameworkMethod method = (FrameworkMethod) iterator.next();
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
            Object test = super.createTest();
            initMethod.invoke(test, fParameters);
            return test;
        }

        @Override
        protected String getName() {
            return String.format("%s with params %s", getTestClass().getName(), ArrayUtils
                    .toString(fParameters));
        }

        @Override
        protected String testName(final FrameworkMethod method) {
            return String.format("%s with params %s", method.getName(), ArrayUtils
                    .toString(fParameters));
        }

        private Method getInitMethod() {
            Method[] methods = ReflectionUtils.getAllDeclaredMethods(getTestClass().getJavaClass());
            for (Method method : methods) {
                if (method.getName().equals("init")
                        && method.getGenericParameterTypes().length == fParameters.length) {
                    return method;
                }
            }
            Assert.fail("Could not find an appropriate method for " + getName());
            return null;
        }

    }

    public ParameterizedSuite(Class<?> klass) throws Exception {
        this(klass, getAnnotatedClasses(klass));
    }

    private final ArrayList<Runner> runners = new ArrayList<Runner>();

    protected ParameterizedSuite(Class<?> klass, Class<?>[] annotatedClasses) throws Exception {
        super(klass, new Class<?>[0]);

        for (final Object each : getParametersList()) {
            if (each instanceof Object[]) {
                for (Class<?> clazz : annotatedClasses) {
                    try {
                        runners.add(new TestClassRunnerForParameters(clazz, (Object[]) each));
                    } catch (Exception ex) {
                        Assert.fail(ex.getMessage() + " for " + clazz.getName());
                    }
                }
            } else {
                throw new Exception(String.format("%s.%s() must return a Collection of arrays.",
                        getTestClass().getName(), getParametersMethod().getName()));
            }
        }
    }

    @Override
    protected List<Runner> getChildren() {
        return runners;
    }

    private static Class<?>[] getAnnotatedClasses(Class<?> klass) throws InitializationError {
        SuiteClasses annotation = klass.getAnnotation(SuiteClasses.class);
        if (annotation == null)
            throw new InitializationError(String.format(
                    "class '%s' must have a SuiteClasses annotation", klass.getName()));
        Class<?>[] classes = new Class[annotation.value().length + 1];
        classes[0] = klass;
        for (int i = 1; i <= annotation.value().length; i++) {
            classes[i] = annotation.value()[i - 1];
        }
        return classes;
    }

    private Collection<?> getParametersList() throws IllegalAccessException,
            InvocationTargetException, Exception {
        return (Collection<?>) getParametersMethod().invoke(null);
    }

    private Method getParametersMethod() throws Exception {
        List<FrameworkMethod> methods = getTestClass().getAnnotatedMethods(Parameters.class);
        for (FrameworkMethod each : methods) {
            int modifiers = each.getMethod().getModifiers();
            if (Modifier.isStatic(modifiers) && Modifier.isPublic(modifiers))
                return each.getMethod();
        }

        throw new Exception("No public static parameters method on class " + getName());
    }

}