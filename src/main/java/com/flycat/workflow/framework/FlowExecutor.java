package com.flycat.workflow.framework;


import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

class FlowExecutor {
    private static final Logger LOGGER = Logger.getLogger(FlowExecutor.class.getName());

    /*
     * Node state.
     */
    private final static class NodeState {
        AtomicInteger barrier;
        FlowNode nextNode;
        FlowNode doneNode;
        Object errorObject;
    }

    private enum ExecutorStatus {
        INIT, RUNNING, EXIT
    }

    private long seqId;
    private ActionContext context;
    private FlowFuture flowFuture;
    private long startTimestamp = 0;
    private long stopTimestamp = 0;
    private AtomicReference<ExecutorStatus> status;
    private Map<FlowNode, NodeState> stateMap;
    private ThreadPoolExecutor threadPool;

    public FlowExecutor(long seqId, ActionContext context,
                        ThreadPoolExecutor executor) {
        this.seqId = seqId;
        this.context = Objects.requireNonNull(context);
        this.flowFuture = new FlowFuture();
        this.status = new AtomicReference<>(ExecutorStatus.INIT);
        this.stateMap = new ConcurrentHashMap<>();
        this.threadPool = Objects.requireNonNull(executor);
    }

    public long getSeqId() { return seqId; }

    public void onExecutorStart() {
        startTimestamp = System.currentTimeMillis();
        status.compareAndSet(ExecutorStatus.INIT, ExecutorStatus.RUNNING);
    }

    public void onExecutorExit() {
        if (status.compareAndSet(ExecutorStatus.RUNNING, ExecutorStatus.EXIT)) {
            flowFuture.setIsDone();
            stopTimestamp = System.currentTimeMillis();
        }
    }

    public Future<Void> getFuture() {
        return flowFuture;
    }

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
                    node.actionExecutor.run(context);
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
                    if (node.flowRunnable != null)
                        node.flowRunnable.run(this);
                    onNodeDone(node);
                    break;
                }
                default: {
                    throw new RuntimeException("Invalid node type: " + node.nodeSeq);
                }
            }
        } catch (Throwable e) {
            setNodeError(node, e);
            LOGGER.log(Level.SEVERE, "FlowExecutor exception: ", e);
        }
    }

    private void setNodeError(FlowNode node, Object o) {
        NodeState nodeState = stateMap.get(node);
        if (nodeState != null) {
            nodeState.errorObject = o;
        }
    }
}
