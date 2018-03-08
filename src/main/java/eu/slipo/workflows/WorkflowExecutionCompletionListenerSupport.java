package eu.slipo.workflows;

import java.util.List;
import java.util.Map;

public class WorkflowExecutionCompletionListenerSupport implements WorkflowExecutionCompletionListener
{
    @Override
    public void onSuccess(WorkflowExecutionSnapshot workflowExecutionSnapshot)
    {
        // no-op
    }

    @Override
    public void onFailure(
        WorkflowExecutionSnapshot workflowExecutionSnapshot, Map<String, List<Throwable>> failureExceptions)
    {
        // no-op
    }
}
