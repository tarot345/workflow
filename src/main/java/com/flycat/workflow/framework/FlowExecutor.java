package com.flycat.workflow.framework;


import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

class FlowExecutor {
    /*
     * Node state.
     */
    private final static class NodeState {
        AtomicInteger barrier;
        FlowNode nextNode;
        FlowNode doneNode;
        Object errorObject;
    }

    private long seqId;
    private ActionContext context;
    private ThreadPoolExecutor threadPool;
    private Map<FlowNode, NodeState> stateMap;

    public FlowExecutor(long seqId, ActionContext context,
                        ThreadPoolExecutor executor) {
        this.seqId = seqId;
        this.context = Objects.requireNonNull(context);
        this.threadPool = Objects.requireNonNull(executor);
        this.stateMap = new ConcurrentHashMap<>();
    }

    public long getSeqId() { return seqId; }

    private void onNodeDone(FlowNode node) {
        NodeState nodeState = stateMap.get(node);
        if (nodeState == null)
            throw new RuntimeException("Invalid workflow state machine");
        int value = nodeState.barrier.decrementAndGet();
        if (value < 0)
            throw new RuntimeException("Invalid workflow state machine");
        if (value == 0) {
            if (nodeState.doneNode != null) {
                onNodeDone(nodeState.doneNode);
            }
            if (nodeState.nextNode != null) {
                runNode(nodeState.nextNode);
            }
        }
    }

    public NodeState addNode(FlowNode node) {
        NodeState nodeState = stateMap.get(node);
        if (nodeState != null)
            throw new RuntimeException("Node already exists: " + node.nodeSeq);
        nodeState = new NodeState();
        stateMap.put(node, nodeState);
        if (node.nodeType == FlowNode.NodeType.ACTION_NODE) {
            nodeState.barrier = new AtomicInteger(1);
        } else if (node.nodeType == FlowNode.NodeType.SERIAL_NODE) {
            ListIterator<FlowNode> it = node.childrenList.listIterator();
            while (it.hasNext()) it.next();
            FlowNode childNode = it.previous();
            NodeState childNodeState = addNode(childNode);
            childNodeState.doneNode = node;
            nodeState.barrier = new AtomicInteger(1);
            FlowNode nextNode = childNode;
            while (it.hasPrevious()) {
                childNode = it.previous();
                childNodeState = addNode(childNode);
                childNodeState.nextNode = nextNode;
                nextNode = childNode;
            }
        } else if (node.nodeType == FlowNode.NodeType.PARALLEL_NODE) {
            for (FlowNode childNode : node.childrenList) {
                NodeState subNodeState = addNode(childNode);
                subNodeState.doneNode = node;
            }
            nodeState.barrier = new AtomicInteger(node.childrenList.size());
        } else if (node.nodeType == FlowNode.NodeType.RUNNABLE_NODE) {
            nodeState.barrier = new AtomicInteger(1);
        }
        return nodeState;
    }

    public void runNode(FlowNode node) {
        try {
            switch (node.nodeType) {
                case ACTION_NODE: {
                    try {
                        node.actionExecutor.run(context);
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                    onNodeDone(node);
                    break;
                }
                case SERIAL_NODE: {
                    runNode(node.childrenList.get(0));
                    break;
                }
                case PARALLEL_NODE: {
                    for (FlowNode childNode : node.childrenList) {
                        threadPool.submit(() -> runNode(childNode));
                    }
                    break;
                }
                case RUNNABLE_NODE: {
                    try {
                        if (node.flowRunnable != null)
                            node.flowRunnable.run(this);
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                    onNodeDone(node);
                    break;
                }
                default: {
                    throw new RuntimeException("Invalid node type: " + node.nodeSeq);
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
            setNodeError(node, e);
        }
    }

    private void setNodeError(FlowNode node, Object o) {
        NodeState nodeState = stateMap.get(node);
        if (nodeState != null) {
            nodeState.errorObject = o;
        }
    }
}
