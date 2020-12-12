package com.flycat.workflow.framework;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;

public class WorkFlowTest {
    private static final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
            100, 100, 60, TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(100));

    public static class TestContext extends ActionContext {
        private final Map<String, Long> actionRecords = new ConcurrentHashMap();
        public void addActionRunRecord(String actionName) {
            actionRecords.put(actionName, System.currentTimeMillis());
        }
        public boolean runsBefore(String actionA, String actionB) {
            Long actionATimestamp = actionRecords.get(actionA);
            Long actionBTimestamp = actionRecords.get(actionB);
            return actionATimestamp != null &&
                    actionBTimestamp != null &&
                    actionATimestamp <= actionBTimestamp;
        }
    }
    public static class TestAction extends Action {
        private TestContext testContext;
        public TestAction(ActionContext context) {
            super(context);
            testContext = (TestContext)context;
        }
        @Override
        public void run() {
            try {
                testContext.addActionRunRecord(this.getClass().getName());
                TimeUnit.MILLISECONDS.sleep(new Random().nextInt(50));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    public static class TestAction001 extends TestAction {
        public TestAction001(ActionContext context) { super(context); }
    }
    public static class TestAction002 extends TestAction {
        public TestAction002(ActionContext context) { super(context); }
    }
    public static class TestAction003 extends TestAction {
        public TestAction003(ActionContext context) { super(context); }
    }
    public static class TestAction004 extends TestAction {
        public TestAction004(ActionContext context) { super(context); }
    }
    public static class TestAction005 extends TestAction {
        public TestAction005(ActionContext context) { super(context); }
    }
    public static class TestAction006 extends TestAction {
        public TestAction006(ActionContext context) { super(context); }
    }

    @Test
    public void workflowTest001() {
        String layout = "{\"name\":\"testWorkflow\",\"workflow\":{\"type\":\"SERIAL\",\"data\":[" +
                "{\"type\":\"ACTION\",\"data\":\"com.flycat.workflow.framework.WorkFlowTest$TestAction001\"}," +
                "{\"type\":\"ACTION\",\"data\":\"com.flycat.workflow.framework.WorkFlowTest$TestAction002\"}," +
                "{\"type\":\"ACTION\",\"data\":\"com.flycat.workflow.framework.WorkFlowTest$TestAction003\"}," +
                "{\"type\":\"ACTION\",\"data\":\"com.flycat.workflow.framework.WorkFlowTest$TestAction004\"}," +
                "{\"type\":\"ACTION\",\"data\":\"com.flycat.workflow.framework.WorkFlowTest$TestAction005\"}," +
                "{\"type\":\"ACTION\",\"data\":\"com.flycat.workflow.framework.WorkFlowTest$TestAction006\"}" +
                "]}}";
        try {
            WorkFlow workFlow = new WorkFlow(layout, threadPool);
            Assert.assertTrue(workFlow.init());
            TestContext testContext = new TestContext();
            Future<Void> future = workFlow.run(testContext);
            future.get(1, TimeUnit.SECONDS);
            Assert.assertTrue(future.isDone());
            Assert.assertTrue(testContext.runsBefore(
                    TestAction001.class.getName(), TestAction002.class.getName()));
            Assert.assertTrue(testContext.runsBefore(
                    TestAction002.class.getName(), TestAction003.class.getName()));
            Assert.assertTrue(testContext.runsBefore(
                    TestAction003.class.getName(), TestAction004.class.getName()));
            Assert.assertTrue(testContext.runsBefore(
                    TestAction004.class.getName(), TestAction005.class.getName()));
            Assert.assertTrue(testContext.runsBefore(
                    TestAction005.class.getName(), TestAction006.class.getName()));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    @Test
    public void workflowTest002() {
        String layout = "{\"name\":\"testWorkflow\",\"workflow\":{\"type\":\"SERIAL\",\"data\":[" +
                "{\"type\":\"PARALLEL\",\"data\":[" +
                    "{\"type\":\"ACTION\",\"data\":\"com.flycat.workflow.framework.WorkFlowTest$TestAction001\"}," +
                    "{\"type\":\"ACTION\",\"data\":\"com.flycat.workflow.framework.WorkFlowTest$TestAction002\"}" +
                "]},{\"type\":\"PARALLEL\",\"data\":[" +
                    "{\"type\":\"ACTION\",\"data\":\"com.flycat.workflow.framework.WorkFlowTest$TestAction004\"}," +
                    "{\"type\":\"ACTION\",\"data\":\"com.flycat.workflow.framework.WorkFlowTest$TestAction005\"}" +
                "]}]}}";
        try {
            WorkFlow workFlow = new WorkFlow(layout, threadPool);
            Assert.assertTrue(workFlow.init());
            TestContext testContext = new TestContext();
            Future<Void> future = workFlow.run(testContext);
            future.get(1, TimeUnit.SECONDS);
            Assert.assertTrue(future.isDone());
            Assert.assertTrue(testContext.runsBefore(
                    TestAction001.class.getName(), TestAction004.class.getName()));
            Assert.assertTrue(testContext.runsBefore(
                    TestAction001.class.getName(), TestAction005.class.getName()));
            Assert.assertTrue(testContext.runsBefore(
                    TestAction002.class.getName(), TestAction004.class.getName()));
            Assert.assertTrue(testContext.runsBefore(
                    TestAction002.class.getName(), TestAction005.class.getName()));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }
}
