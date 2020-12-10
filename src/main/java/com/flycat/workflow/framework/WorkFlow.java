package com.flycat.workflow.framework;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

/************************************************************************
 *
 * workflow layout text demo(json format):
 *
 * {
 *  "name": "fly-cat",
 *  "workflow": {
 *      "type": "SERIAL",
 *      "data": [
 *          {
 *              "type": "ACTION",
 *              "data": "com.flycat.biz.InitTask"
 *          },
 *          {
 *              "type": "PARALLEL",
 *              "data": [
 *                  {
 *                      "type": "ACTION",
 *                      "data": "com.flycat.biz.SearchAction"
 *                  },
 *                  {
 *                      "type": "ACTION",
 *                      "data": "com.flycat.biz.UploadAction"
 *                  }
 *              ]
 *          },
 *          {
 *              "type": "SERIAL",
 *              "data": [
 *                  {
 *                      "type": "ACTION",
 *                      "data": "com.flycat.biz.DumpAction"
 *                  },
 *                  {
 *                      "type": "ACTION",
 *                      "data": "com.flycat.biz.LogAction"
 *                  }
 *              ]
 *          }
 *      ]
 *   }
 * }
 *
 ***********************************************************************/

public class WorkFlow {
    private String name;
    private String layout;
    private FlowNode rootNode;
    private AtomicLong counter;
    private ThreadPoolExecutor threadPool;

    public WorkFlow(String layoutConfig, ThreadPoolExecutor threadPool) {
        this.layout = Objects.requireNonNull(layoutConfig);
        this.counter = new AtomicLong(0);
        this.threadPool = Objects.requireNonNull(threadPool);
    }

    public boolean init() {
        try {
            rootNode = new FlowNode("root");
            rootNode.nodeType = FlowNode.NodeType.SERIAL_NODE;
            rootNode.childrenList = new LinkedList<>();

            FlowNode startNode = new FlowNode("start");
            startNode.nodeType = FlowNode.NodeType.RUNNABLE_NODE;
            startNode.flowRunnable = (FlowExecutor e) -> { onFlowExecutorStart(e); };
            rootNode.childrenList.add(startNode);

            JSONObject layoutObject = JSONObject.parseObject(layout);
            name = Objects.requireNonNull(layoutObject.getString("name"));
            JSONObject rootObject = layoutObject.getJSONObject("workflow");
            FlowNode bizNode = parseLayoutNode(rootObject, "0");
            rootNode.childrenList.add(bizNode);

            FlowNode exitNode = new FlowNode("exit");
            exitNode.nodeType = FlowNode.NodeType.RUNNABLE_NODE;
            exitNode.flowRunnable = (FlowExecutor e) -> { onFlowExecutorExit(e);};
            rootNode.childrenList.add(exitNode);
        } catch (Throwable e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private void onFlowExecutorStart(FlowExecutor executor) {
        System.out.println("Executor " + executor.getSeqId() + " start");
    }

    private void onFlowExecutorExit(FlowExecutor executor) {
        System.out.println("Executor " + executor.getSeqId() + " exit");
    }

    private FlowNode parseLayoutNode(JSONObject object, String nodeSeq) throws Exception {
        FlowNode node = new FlowNode(nodeSeq);
        String nodeType = object.getString("type");
        if (nodeType == null)
            throw new RuntimeException("No type field in workflow node " + nodeSeq);
        if (FlowNode.NodeType.ACTION_NODE.name.equals(nodeType)) {
            node.nodeType = FlowNode.NodeType.ACTION_NODE;
            String actionClassPath = object.getString("data");
            if (actionClassPath == null)
                throw new RuntimeException("No data field in workflow node " + nodeSeq);
            Class<?> actionClass = Class.forName(actionClassPath);
            if (!Action.class.isAssignableFrom(actionClass))
                throw new RuntimeException("Invalid action class in node " + nodeSeq);
            node.actionExecutor = new ActionExecutor((Class<? extends Action>)actionClass);
        } else if (FlowNode.NodeType.SERIAL_NODE.name.equals(nodeType)) {
            node.nodeType = FlowNode.NodeType.SERIAL_NODE;
            JSONArray childArray = object.getJSONArray("data");
            if (childArray == null || childArray.isEmpty())
                throw new RuntimeException("No data field in workflow node " + nodeSeq);
            node.childrenList = new ArrayList<>(childArray.size());
            for (int i = 0; i < childArray.size(); ++i) {
                JSONObject childObject = childArray.getJSONObject(i);
                node.childrenList.add(parseLayoutNode(childObject, nodeSeq + "-" + i));
            }
        } else if (FlowNode.NodeType.PARALLEL_NODE.name.equals(nodeType)) {
            node.nodeType = FlowNode.NodeType.PARALLEL_NODE;
            JSONArray childArray = object.getJSONArray("data");
            if (childArray == null || childArray.isEmpty())
                throw new RuntimeException("Invalid data field in workflow node " + nodeSeq);
            node.childrenList = new ArrayList<>(childArray.size());
            for (int i = 0; i < childArray.size(); ++i) {
                JSONObject childObject = childArray.getJSONObject(i);
                node.childrenList.add(parseLayoutNode(childObject, nodeSeq + "-" + i));
            }
        } else {
            throw new RuntimeException("Invalid type field in workflow node " + nodeSeq);
        }
        return node;
    }

    public void run(ActionContext context) {
        FlowExecutor flowExecutor = new FlowExecutor(
                counter.incrementAndGet(), context, threadPool);
        flowExecutor.addNode(rootNode);
        flowExecutor.runNode(rootNode);
    }
}
