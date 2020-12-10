package com.flycat.workflow.framework;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Objects;

class ActionExecutor {
    private Class<? extends Action> actionClass;
    private Constructor<? extends Action> constructor;
    private Method checkMethod, runMethod;

    public ActionExecutor(Class<? extends Action> clazz) {
        actionClass = Objects.requireNonNull(clazz);
        try {
            constructor = actionClass.getDeclaredConstructor(ActionContext.class);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException(
                    "No valid action constructor in class " + actionClass.getName());
        }
        try {
            checkMethod = actionClass.getMethod("check", ActionContext.class);
            if (!Modifier.isStatic(checkMethod.getModifiers()))
                throw new IllegalArgumentException("Invalid static check method");
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException(
                    "No valid static check() function in class " + actionClass.getName());
        }
        try {
            runMethod = actionClass.getMethod("run");
            if (!Modifier.isPublic(runMethod.getModifiers()) ||
                    Modifier.isStatic(runMethod.getModifiers())) {
                throw new RuntimeException("Invalid action run method");
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException(
                    "No valid run() method in class " + actionClass.getName());
        }
    }

    public void run(ActionContext context) {
        try {
            if ((Boolean)checkMethod.invoke(actionClass, context)) {
                Action action = constructor.newInstance(context);
                runMethod.invoke(action);
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
