package eu.slipo.workflows.tests.unit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.collections4.ListUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import eu.slipo.workflows.Workflow;
import eu.slipo.workflows.WorkflowBuilderFactory;
import eu.slipo.workflows.WorkflowExecutionEventListener;
import eu.slipo.workflows.WorkflowExecutionEventListenerSupport;
import eu.slipo.workflows.WorkflowExecutionListener;
import eu.slipo.workflows.WorkflowExecutionSnapshot;
import eu.slipo.workflows.tests.BatchConfiguration;
import eu.slipo.workflows.tests.TaskExecutorConfiguration;
import eu.slipo.workflows.tests.WorkflowBuilderFactoryConfiguration;

@RunWith(SpringRunner.class)
@EnableAutoConfiguration
@SpringBootTest(
    classes = { 
        TaskExecutorConfiguration.class, 
        BatchConfiguration.class,
        WorkflowBuilderFactoryConfiguration.class
    })
public class WorkflowBuilderTests
{    
    private static class DummyTasklet implements Tasklet
    {
        @Override
        public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
            throws Exception
        {
            System.err.println("Executing dummy step ...");
            return null;
        }
    }
    
    private static Path inputPath = Paths.get("/var/local/slipo-workbench/1.txt");
    
    @Autowired
    @Qualifier("workflowDataDirectory")
    private Path dataDir;
    
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    
    @Autowired
    private WorkflowBuilderFactory workflowBuilderFactory;
    
    private Step dummyStep;

    private WorkflowExecutionEventListener listener;
    
    private JobExecutionListener listenerAlpha;
    
    private UUID workflowId;
    
    private Workflow workflow;
    
    @Before
    public void setUp() throws Exception
    {
        workflowId = UUID.randomUUID();
        
        dummyStep = stepBuilderFactory.get("dummy")
            .tasklet(new DummyTasklet())
            .build();
        
        listener = new WorkflowExecutionEventListenerSupport() {};
        
        listenerAlpha = new JobExecutionListenerSupport() {};
        
        workflow = workflowBuilderFactory.get(workflowId)
            .job(c -> c
                .name("alpha")
                .flow(dummyStep)
                .input(inputPath)
                .output("a1.txt", "a2.txt")
                .parameters(b -> b
                    .addLong("number", 199L)
                    .addString("greeting", "Hello World")))
            .job(c -> c
                .name("xray")
                .flow(dummyStep)
                .parameters(b -> b.addString("foo", "Baz"))
                .output("x1.txt"))
            .job(c -> c
                .name("bravo")
                .flow(dummyStep)
                .input("alpha", "*.txt")
                .output("b1.txt", "b2.txt"))
            .job(c -> c
                .name("charlie")
                .flow(dummyStep)
                .input("alpha", "a1.txt")
                .output("c1.txt", "c2.txt", "c3.txt"))
            .output("bravo", "b1.txt", "res-b-1.txt")
            .output("charlie", "c1.txt", "res-c-1.txt")
            .listener(listener)
            .listener("alpha", listenerAlpha)
            .build();
    }

    @After
    public void tearDown() throws Exception
    {
        workflowId = null;
        workflow = null;
    }
    
    @Test
    public void testIdentifier()
    {        
        assertEquals(workflowId, workflow.id());
    }
    
    @Test
    public void testNodesAndDependencies()
    {   
        assertEquals(
            new HashSet<>(Arrays.asList("alpha", "bravo", "charlie", "xray")), workflow.nodeNames());
        
        Workflow.JobNode nodeA = workflow.node("alpha"), 
            nodeB = workflow.node("bravo"), 
            nodeC = workflow.node("charlie"), 
            nodeX = workflow.node("xray");
        
        assertEquals(nodeA.name(), "alpha");
        assertEquals(nodeB.name(), "bravo");
        assertEquals(nodeC.name(), "charlie");
        assertEquals(nodeX.name(), "xray");
        
        // Test dependencies
        
        assertEquals(Collections.emptyList(), IterableUtils.toList(nodeA.dependencies()));
        assertEquals(Arrays.asList(nodeB, nodeC), IterableUtils.toList(nodeA.dependents()));

        assertEquals(Collections.singletonList(nodeA), IterableUtils.toList(nodeB.dependencies()));
        assertEquals(Collections.emptyList(), IterableUtils.toList(nodeB.dependents()));
        
        assertEquals(Collections.singletonList(nodeA), IterableUtils.toList(nodeC.dependencies()));
        assertEquals(Collections.emptyList(), IterableUtils.toList(nodeC.dependents()));
        
        assertEquals(Collections.emptyList(), IterableUtils.toList(nodeX.dependencies()));
        assertEquals(Collections.emptyList(), IterableUtils.toList(nodeX.dependents()));
        
        // Test inputs and outputs for nodes
       
        for (Workflow.JobNode node: workflow.nodes()) {
            Path stagingDir = workflow.stagingDirectory(node.name());
            assertTrue("Expected output of node to be inside its own staging directory", 
                IterableUtils.matchesAll(node.output(), p -> p.startsWith(stagingDir)));
        }
        
        assertEquals(Collections.singletonList(inputPath), nodeA.input());
        
        assertTrue("Expected input of `bravo` to contain output of `alpha`", 
            nodeB.input().containsAll(nodeA.output()));
        
        assertTrue("Expected input of `charlie` to contain output of `alpha` named a1.txt", 
            nodeC.input().containsAll(
                ListUtils.select(nodeA.output(), p -> p.getFileName().toString().equals("a1.txt"))));
        
        assertEquals(Collections.emptyList(), nodeX.input());
        
        // Test job-name, flow, and parameters
     
        for (Workflow.JobNode node: workflow.nodes()) {
            final JobParameters parameters = node.parameters();
            final String jobName = node.jobName();
            final Flow flow = node.flow(stepBuilderFactory);
            assertNotNull("A node is expected to have a job name", jobName);
            assertNotNull("A node is expected to be associated to Batch flow", flow);
            assertNotNull("Expected a parameter fpr the workflow identifier", 
                parameters.getString(Workflow.Parameter.WORKFLOW.key()));
            List<Path> inputs = Arrays.stream(
                    parameters.getString(Workflow.Parameter.INPUT.key()).split(File.pathSeparator))
                .filter(s -> !s.isEmpty())
                .map(s -> Paths.get(s))
                .collect(Collectors.toList());
            assertEquals(inputs, node.input());
        }
        
        assertEquals(nodeA.parameters().getString("greeting"), "Hello World");
        assertEquals(nodeA.parameters().getLong("number"), Long.valueOf(199L));
        assertEquals(nodeX.parameters().getString("foo"), "Baz");
    }
    
    @Test(expected = IllegalArgumentException.class) 
    public void testNonExistingName()
    {
        workflow.node("zulu");
    }
    
    @Test(expected = UnsupportedOperationException.class) 
    public void testUnmodifiableNames()
    {
        workflow.nodeNames().add("zulu");
    }
    
    @Test(expected = UnsupportedOperationException.class) 
    public void testUnmodifiableOutputMapOfPaths()
    {
        workflow.output().put("foo.txt", Paths.get("baz.txt"));
    }
    
    @Test(expected = UnsupportedOperationException.class) 
    public void testUnmodifiableOutputMapOfUris()
    {
        workflow.outputUris().put("foo.txt", URI.create("file:///tmp/baz.txt"));
    }
    
    @Test
    public void testNodesInTopologicalOrder()
    {
        List<String> names = IterableUtils.toList(
            IterableUtils.transformedIterable(
                workflow.nodesInTopologicalOrder(),
                y -> y.name()));
        
        assertTrue("Expected node `alpha` to precede node `bravo`", 
            names.indexOf("alpha") < names.indexOf("bravo"));
        assertTrue("Expected node `alpha` to precede node `charlie`", 
            names.indexOf("alpha") < names.indexOf("charlie"));
    }
    
    @Test
    public void testOutputMap()
    {
        Map<String,Path> outputMap = workflow.output();
        
        assertEquals(
            new HashSet<>(Arrays.asList("res-b-1.txt", "res-c-1.txt")),
            outputMap.keySet());
        
        assertTrue(
            outputMap.get("res-b-1.txt").startsWith(workflow.stagingDirectory("bravo")));
        assertTrue(
            outputMap.get("res-b-1.txt").endsWith("b1.txt"));
        assertTrue(
            outputMap.get("res-c-1.txt").startsWith(workflow.stagingDirectory("charlie")));
        assertTrue(
            outputMap.get("res-c-1.txt").endsWith("c1.txt"));
    }
    
    @Test
    public void testPaths()
    {
        Path workflowDir = workflow.dataDirectory();
        Path workflowOutputDir = workflow.outputDirectory();
        
        assertTrue("Expected an absolute path", dataDir.isAbsolute());
        assertTrue("Expected an absolute path", workflowDir.isAbsolute());
        
        assertTrue("Expected workflow\'s data directory inside given data directory", 
            workflowDir.startsWith(dataDir));
        assertTrue("Expected output directory inside workflow\'s data directory", 
            workflowOutputDir.startsWith(workflowDir));
        
        for (String nodeName: workflow.nodeNames()) {
            assertTrue("Expected node\'s staging directory inside workflow\'s data directory", 
                workflow.stagingDirectory(nodeName).startsWith(workflowDir));
        }
    }

    @Test
    public void testListeners()
    {
        assertTrue(workflow.getListeners().size() == 1);
        assertTrue(workflow.getListeners("alpha").size() == 1);
        assertTrue(workflow.getListeners("bravo").isEmpty());
        assertTrue(workflow.getListeners("charlie").isEmpty());
        assertTrue(workflow.getListeners("xray").isEmpty());
        assertTrue(workflow.getListeners("zulu").isEmpty());
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testUnmodifiableListeners1()
    {
        workflow.getListeners().add(new WorkflowExecutionListener() {});
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testUnmodifiableListeners2()
    {
        workflow.getListeners("alpha").add(new WorkflowExecutionListener() {});
    }
    
}
