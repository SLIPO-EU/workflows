# Slipo Workflows

Model and execute workflows atop of Spring-Batch jobs.

Roughly, a *workflow* is a graph of Spring-Batch flows. A flow *A* is connected to flow *B* if it depends on (i.e. operates on) some output expected to be generated by flow *B*.  

## Build

Before building the package, we must create some temporary directories needed for unit/integration tests (if these dont suit you, edit accordingly the testing configuration under `src/test/resources/config/application.properties`):

    mkdir -p ~/var/slipo-workbench/jobs/ ~/var/slipo-workbench/workflows/

Build as a common maven module:

    mvn package

Run integration tests (this may take some time):

    mvn -P integration-tests verify

## Requirements for a flow

The workflow scheduler analyzes dependencies and executes participating flows in the proper order, trying to parallelize execution as much as possible. Each flow is wrapped into a Spring-Batch job and is executed as usual. The main duty of this wrapper job (job *node*) is to copy/link the output of a flow and make it accessible to other (dependent) flows via a workflow-wide staging area.

In order for a Spring-Batch flow to participate in a workflow, it must adhere to the following requirements:

  * must accept its input only via an `input` or `input.<key>` job parameter. This parameter must be encoded as a semicolon-separated list of absolute file paths.
  * must publish an `outputDir` entry to its execution context. This entry must contain the absolute path of the job-wide output directory, and this will be used to resolve all outputs expected by this job node.      

## Build a workflow

A workflow is built on top of Spring Batch flows. An example taken from project's tests:

```java

UUID workflowId = UUID.randomUUID();
Workflow workflow = workflowBuilderFactory.get(workflowId)
    .job(b -> b.name("splitter")
        .flow(splitFileFlow)
        .input(Paths.get("/tmp/input/numbers.txt"))
        .parameters(p -> p.addLong("numParts", 2L)
            .addString("outputPrefix", "part").addString("outputSuffix", ".txt"))
        .output("part1.txt", "part2.txt"))
    .job(b -> b.name("validator")
        .flow(validatorStep)
        .input("splitter", "part1.txt")
        .input("splitter", "part2.txt"))
    .job(b -> b.name("sorter-1")
        .flow(sortFileFlow)
        .after("validator")
        .input("splitter", "part1.txt")
        .parameters(p -> p.addString("outputName", "r.txt"))
        .output("r.txt"))
    .job(b -> b.name("sorter-2")
        .flow(sortFileFlow)
        .after("validator")
        .input("splitter", "part2.txt")
        .parameters(p -> p.addString("outputName", "r.txt"))
        .output("r.txt"))
    .job(b -> b.name("merger")
        .flow(mergeFilesFlow)
        .input("sorter-1", "r.txt")
        .input("sorter-2", "r.txt")
        .parameters(p -> p.addString("outputName", "r.txt"))
        .output("r.txt"))
    .output("merger", "r.txt")
    .build();

```

## Execute a workflow

A workflow is executed (started or restarted) by a workflow scheduler (interface `eu.slipo.workflows.service.WorkflowScheduler`). Currently, the only implementation is the
`EventBasedWorkflowScheduler`.

