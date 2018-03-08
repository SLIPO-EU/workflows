package eu.slipo.workflows;

import org.springframework.batch.core.JobExecution;

public interface WorkflowExecutionEventListener extends WorkflowExecutionCompletionListener
{
    /**
     * Take action before a job node is started. 
     * 
     * @param workflowExecutionSnapshot
     * @param nodeName
     * @param jobExecution
     */
    void beforeNode(
        WorkflowExecutionSnapshot workflowExecutionSnapshot, String nodeName, JobExecution jobExecution);
    
    /**
     * Take action after a job node is finished (completed successfully, failed or interrupted).  
     * 
     * @param workflowExecutionSnapshot
     * @param nodeName
     * @param jobExecution
     */
    void afterNode(
        WorkflowExecutionSnapshot workflowExecutionSnapshot, String nodeName, JobExecution jobExecution);
    
    /**
     * An optional filter for the set of nodes we care about.
     * 
     * @param nodeName
     * @return
     */
    default boolean aboutNode(String nodeName)
    {
        return true;
    }
}
