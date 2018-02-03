# Slipo Workflows

Model and execute workflows atop of Spring-Batch jobs.

Roughly, a *workflow* is a graph of Spring-Batch flows. A flow *A* is connected to flow *B* if it depends on (i.e. operates on) some output expected to be generated by flow *B*.  

## Requirements for a flow

The workflow scheduler analyzes dependencies and executes participating flows in the proper order, trying to parallelize execution as much as possible. Each flow is wrapped into a Spring-Batch job and is executed as usual. The main duty of this wrapper job (job *node*) is to copy/link the output of a flow and make it accessible to other (dependent) flows via a workflow-wide staging area.

In order for a Spring-Batch flow to participate in a workflow, it must adhere to the following requirements:

  * must accept its input only via a `input` job parameter. This parameter must be encoded as a semicolon-separated list of absolute file paths.
  * must publish an `outputDir` entry to its execution context. This entry must contain the absolute path of the job-wide output directory, and this will be used to resolve all outputs expected by this job node.      

## Describe a workflow

__Todo__

## Execute a workflow

__Todo__
