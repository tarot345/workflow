package com.flycat.workflow.framework;

import java.util.Objects;

public class Action {

    protected ActionContext context;

    /*
     * Construct action with a context.
     */
    public Action(ActionContext context) {
        this.context = Objects.requireNonNull(context);
    }

    /*
     * Check whether need run this action.
     */
    public static boolean check(ActionContext context) { return true; }

    /*
     * Run this action, please make sure capture all internal exceptions.
     */
    public void run() {}
}
