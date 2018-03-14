package eu.slipo.workflows;

import java.util.Set;

import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;

public class ExecutionContextPromotionListeners
{
    private ExecutionContextPromotionListeners() {}
    
    public static StepExecutionListener fromKeys(Set<String> keys)
    {
        return fromKeys(keys.toArray(new String[0]));
    }
    
    public static StepExecutionListener fromKeys(String ...keys)
    {
        ExecutionContextPromotionListener listener = new ExecutionContextPromotionListener();
        listener.setKeys(keys);
        listener.setStrict(true);
        
        try {
            listener.afterPropertiesSet();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        
        return listener;
    }
}
