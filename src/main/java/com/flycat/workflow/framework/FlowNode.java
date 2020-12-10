package com.flycat.workflow.framework;

import java.util.List;
import java.util.Objects;

class FlowNode {
    enum NodeType {
        ACTION_NODE("ACTION"),
        SERIAL_NODE("SERIAL"),
        PARALLEL_NODE("PARALLEL"),
        FLOW_TASK_NODE("_TASK_");

        String name;
        NodeType(String name) { this.name = name; }
    }

    public FlowNode() {this.nodeSeq = "0"; }
    public FlowNode(String seq) { this.nodeSeq = Objects.requireNonNull(seq); }

    String nodeSeq;
    NodeType nodeType;
    ActionExecutor actionExecutor;
    List<FlowNode> childrenList;
    FlowTask flowTask;
}
