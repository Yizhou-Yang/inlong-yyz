package com.tencent.subscribe.format.protobuf;

public class MoreRunnables {


    public static <EXCEPTION extends Throwable> void throwing(ThrowableRunable<EXCEPTION> throwableRunable) {
        try {
            throwableRunable.run();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

}
