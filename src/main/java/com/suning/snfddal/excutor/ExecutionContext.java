package com.suning.snfddal.excutor;


/**
 * 
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public interface ExecutionContext<T> {
    
    public boolean isAutoCommit();
        
    public T getResult();
    
    public void submitCall();
}
