package eu.slipo.workflows.tests;

import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;

public class ExecutionContextPromotionListeners
{
    private ExecutionContextPromotionListeners() {}
    
    public static StepExecutionListener fromKeys(String ...keys) 
        throws Exception
    {
        ExecutionContextPromotionListener listener = new ExecutionContextPromotionListener();
        listener.setKeys(keys);
        listener.setStrict(true);
        listener.afterPropertiesSet();
        return listener;
    }
}
