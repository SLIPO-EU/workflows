package eu.slipo.workflows.service;

import java.util.UUID;

import eu.slipo.workflows.Workflow;
import eu.slipo.workflows.WorkflowExecutionStatus;
import eu.slipo.workflows.WorkflowExecutionCompletionListener;
import eu.slipo.workflows.WorkflowExecutionSnapshot;
import eu.slipo.workflows.WorkflowExecutionStopListener;
import eu.slipo.workflows.exception.WorkflowExecutionStartException;
import eu.slipo.workflows.exception.WorkflowExecutionStopException;

public interface WorkflowScheduler
{
    /**
     * A scheduler-level snapshot of a workflow execution
     */
    interface ExecutionSnapshot
    {
        WorkflowExecutionStatus status();
        
        WorkflowExecutionSnapshot workflowExecutionSnapshot();
    }
    
    /**
     * Start (or restart) a workflow.
     * 
     * @param workflow The model (specification) of the workflow
     * @param callbacks An array 
     * @throws WorkflowExecutionStartException  
     */
    void start(Workflow workflow, WorkflowExecutionCompletionListener... callbacks) 
        throws WorkflowExecutionStartException;
    
    /**
     * Stop a workflow execution. 
     * 
     * @param workflowId The id of the workflow
     * @param callbacks
     * @throws WorkflowExecutionStopException
     */
    void stop(UUID workflowId, WorkflowExecutionStopListener... callbacks) 
        throws WorkflowExecutionStopException;
    
    /**
     * Take a snapshot of workflow execution
     * 
     * @param workflowId The id of the workflow
     */
    ExecutionSnapshot poll(UUID workflowId);
    
    /**
     * Get the scheduler-level status of the workflow execution
     * 
     * @param workflowId The id of the workflow
     */
    WorkflowExecutionStatus status(UUID workflowId);
    
    /**
     * Fetch information on a workflow execution.
     * 
     * <p>This is similar to {@link WorkflowScheduler#poll(UUID)}, but instead returns a 
     * DTO object which is more suitable (e.g is serializable) for exchange between other 
     * services or controllers (or remote clients).
     * 
     * @param workflowId The id of the workflow
     */
    WorkflowExecutionInfo info(UUID workflowId);
    
    /**
     * List all known workflow identifiers
     */
    Iterable<UUID> list();
}
