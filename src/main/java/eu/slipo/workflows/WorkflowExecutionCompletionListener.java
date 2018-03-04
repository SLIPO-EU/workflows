package eu.slipo.workflows;

import java.util.List;
import java.util.Map;

public interface WorkflowExecutionCompletionListener extends WorkflowExecutionListener
{
    /**
     * Handle successful completion of a workflow execution
     * 
     * @param workflowExecutionSnapshot A snapshot of the workflow execution 
     */
    void onSuccess(WorkflowExecutionSnapshot workflowExecutionSnapshot);
    
    /**
     * Handle failure of a workflow execution (at least one node is failed).
     * 
     * @param workflowExecutionSnapshot A snapshot of the workflow execution
     * @param failureExceptions A list of exceptions mapped by node name
     */
    void onFailure(
        WorkflowExecutionSnapshot workflowExecutionSnapshot, 
        Map<String, List<Throwable>> failureExceptions);
}
