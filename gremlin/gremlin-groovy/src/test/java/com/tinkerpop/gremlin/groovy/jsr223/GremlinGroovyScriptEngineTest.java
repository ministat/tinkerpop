package com.tinkerpop.gremlin.groovy.jsr223;

import org.junit.Test;

import javax.script.ScriptException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinGroovyScriptEngineTest {
    @Test
    public void shouldReloadClassLoaderWhileDoingEvalInSeparateThread() throws Exception {
        final AtomicBoolean fail = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);
        final GremlinGroovyScriptEngine scriptEngine = new GremlinGroovyScriptEngine();
        final Thread t = new Thread(() -> {
            try {
                final Object o = scriptEngine.eval("Color.BLACK");
                System.out.println("Should not print: " + o);
                fail.set(true);
            } catch (ScriptException se) {
                // should get here as Color.BLACK is not imported yet.
                System.out.println("Failed to execute Color.BLACK as expected.");
            }

            try {
                int counter = 0;
                while (latch.getCount() == 1) {
                    scriptEngine.eval("1+1");
                    counter++;
                }

                System.out.println(counter + " executions.");

                scriptEngine.eval("Color.BLACK");
                System.out.println("Color.BLACK now evaluates");
            } catch (Exception se) {
                se.printStackTrace();
                fail.set(true);
            }
        });

        t.start();

        // let the first thead execute a bit.
        Thread.sleep(1000);

        new Thread(() -> {
            System.out.println("Importing java.awt.Color...");
            final Set<String> imports = new HashSet<String>() {{
                add("import java.awt.Color");
            }};
            scriptEngine.addImports(imports);
            latch.countDown();
        }).start();

        t.join();

        assertFalse(fail.get());
    }
}
