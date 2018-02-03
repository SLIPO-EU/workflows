package eu.slipo.workflows;

import org.springframework.batch.core.BatchStatus;

/**
 * The overall status of a workflow execution (as aggregated from nodes that 
 * comprise the workflow).
 */
public enum WorkflowExecutionStatus
{
    /** 
     * The workflow is started and running (at least one node is running). 
     * 
     * <p>Note: There may be failed nodes (i.e. at {@link BatchStatus#FAILED}), but the 
     * workflow is not yet characterized as failed (this will happen as soon as
     * all executions are finished).  
     */
    RUNNING, 
            
    /**
     * The workflow is stopped (nothing is running) due to a stop request.
     */
    STOPPED, 
    
    /**
     * The workflow is not running and at least one node reports as failed 
     * (i.e. {@link BatchStatus#FAILED}).
     */
    FAILED,
    
    /**
     * The workflow is not running and every node reports as complete (i.e at
     * {@link BatchStatus#COMPLETED})
     */
    COMPLETED;
}