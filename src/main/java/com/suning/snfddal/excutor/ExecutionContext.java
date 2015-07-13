package com.suning.snfddal.excutor;


/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public interface ExecutionContext<T> {

    boolean isAutoCommit();

    T getResult();

    void submitCall();
}
