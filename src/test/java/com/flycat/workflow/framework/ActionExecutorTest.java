package com.flycat.workflow.framework;

import org.junit.Assert;
import org.junit.Test;

public class ActionExecutorTest {

    private static class TestContext extends ActionContext {
        private boolean needRun = false;
        private boolean run = false;

        public void setNeedRun(boolean b) { needRun = b; }
        public boolean getNeedRun() { return needRun; }
        public void setRun() { run = true; }
        public boolean getRun() { return run; }
    }

    private static class TestAction extends Action {
        private TestContext testContext;
        public TestAction(ActionContext context) {
            super(context);
            testContext = (TestContext)context;
        }
        public static boolean check(ActionContext context) {
            TestContext testContext = (TestContext)context;
            return testContext.getNeedRun();
        }
        @Override
        public void run() {
            testContext.setRun();
        }
    }

    @Test
    public void executorTest() {
        try {
            TestContext testContext = new TestContext();
            ActionExecutor executor = new ActionExecutor(TestAction.class);

            testContext.setNeedRun(false);
            executor.run(testContext);
            Assert.assertFalse(testContext.getRun());

            testContext.setNeedRun(true);
            executor.run(testContext);
            Assert.assertTrue(testContext.getRun());
        } catch (Exception e) {
            Assert.assertTrue(false);
        }
    }
}
