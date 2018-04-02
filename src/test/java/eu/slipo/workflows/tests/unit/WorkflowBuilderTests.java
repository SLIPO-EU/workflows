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
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.collections4.ListUtils;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
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
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.junit4.SpringRunner;

import static org.apache.commons.collections4.IterableUtils.toList;

import eu.slipo.workflows.Workflow;
import eu.slipo.workflows.WorkflowBuilderFactory;
import eu.slipo.workflows.WorkflowExecutionEventListener;
import eu.slipo.workflows.WorkflowExecutionEventListenerSupport;
import eu.slipo.workflows.WorkflowExecutionListener;
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
    },
    webEnvironment = WebEnvironment.NONE)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
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
    
    @TestConfiguration
    public static class Configuration
    {
        @Bean
        public Path inputPath1()
        {
            return Paths.get("/var/local/slipo-workbench/1.txt");
        }
        
        @Bean
        public Path inputPath2a()
        {
            return Paths.get("/var/local/slipo-workbench/2-a.txt");
        }
        
        @Bean
        public Path inputPath2b()
        {
            return Paths.get("/var/local/slipo-workbench/2-b.txt");
        }
        
        @Bean
        public Path inputPath2R()
        {
            return Paths.get("/var/local/slipo-workbench/2-rules.xml");
        }
        
        @Bean
        public UUID workflowId1()
        {
            return UUID.randomUUID();
        }
        
        @Bean
        public UUID workflowId2()
        {
            return UUID.randomUUID();
        }
        
        @Bean
        public Step dummyStep(StepBuilderFactory stepBuilderFactory)
        {
            return stepBuilderFactory.get("dummy").tasklet(new DummyTasklet()).build();
        }
        
        @Bean
        public JobExecutionListener alphaListener()
        {
            return new JobExecutionListenerSupport() {};
        }
        
        @Bean 
        public WorkflowExecutionEventListener globalListener()
        {
            return new WorkflowExecutionEventListenerSupport() {};
        }
        
        @Bean
        public Date datestamp1()
        {
            return new Date();
        }
        
        @Bean
        public Workflow workflow1(
            WorkflowBuilderFactory workflowBuilderFactory,
            UUID workflowId1, Path inputPath1, Step dummyStep, Date datestamp1,
            WorkflowExecutionEventListener globalListener, JobExecutionListener alphaListener)
        {
            return workflowBuilderFactory.get(workflowId1)
                .job(c -> c.name("alpha")
                    .flow(dummyStep)
                    .input(inputPath1)
                    .output("a1.txt", "a2.txt")
                    .parameters(b -> b
                        .addLong("number", 199L).addString("greeting", "Hello World"))
                    .exportToContext("numberOfErrors", "numberOfWarnings"))
                .job(c -> c.name("xray")
                    .flow(dummyStep)
                    .parameters(Collections.singletonMap("magic", 1997))
                    .output("x1.txt"))
                .job(c -> c.name("alpha-validator")
                    .flow(dummyStep)
                    .parameters(b -> b.addString("strict", "true"))
                    .input("alpha", "*.txt"))
                .job(c -> c.name("bravo")
                    .flow(dummyStep)
                    .parameters(Collections.singletonMap("now", datestamp1))
                    .after("alpha-validator")
                    .input("alpha", "*.txt")
                    .contextFrom("alpha", "numberOfErrors", "alpha.numberOfErrors")
                    .output("b1.txt", "b2.txt"))
                .job(c -> c.name("charlie")
                    .flow(dummyStep)
                    .parameters(Collections.singletonMap("pi", Math.PI))
                    .after("alpha-validator")
                    .input("alpha", "a1.txt")
                    .output("c1.txt", "c2.txt", "c3.txt"))
                .output("bravo", "b1.txt", "res-b-1.txt")
                .output("charlie", "c1.txt", "res-c-1.txt")
                .listener(globalListener)
                .listener("alpha", alphaListener)
                .listener("alpha-validator", alphaListener)
                .build();
        }
        
        @Bean
        public Workflow workflow2(
            WorkflowBuilderFactory workflowBuilderFactory,
            UUID workflowId2, Path inputPath2a, Path inputPath2b, Path inputPath2R, Step dummyStep)
        {
            return workflowBuilderFactory.get(workflowId2)
                .job(c -> c.name("interlink")
                    .parameters(p -> p.addDouble("threshold", 0.95).addString("m1", "v1"))
                    .flow(dummyStep)
                    .input(inputPath2R, "rules")
                    .input("transform-A", "*.txt", "A")
                    .input("transform-B-P2", "*.txt", "B")
                    .output("result.txt", "review.txt"))
                // Input from group "A" has a single (logical) step of processing
                .job(c -> c.name("transform-A")
                    .parameters(p -> p.addString("a1", "v1"))
                    .flow(dummyStep)
                    .input(inputPath2a)
                    .output("a.txt"))
                // Input from group "B" takes 2 (logical) processing steps (P1,P2) 
                .job(c -> c.name("transform-B-P1")
                    .parameters(p -> p.addString("b11", "u11"))
                    .flow(dummyStep)
                    .input(inputPath2b)
                    .output("b1.txt"))
                .job(c -> c.name("transform-B-P2")
                    .parameters(p -> p.addString("b21", "u21"))
                    .flow(dummyStep)
                    .input("transform-B-P1", "b1.txt")
                    .output("b2.txt"))
                .build();
        }
    }
    
    @Autowired
    @Qualifier("workflowDataDirectory")
    private Path dataDir;
    
    @Autowired
    private StepBuilderFactory stepBuilderFactory;  
    
    @Autowired
    private Path inputPath1;
     
    @Autowired
    private Path inputPath2a;
    
    @Autowired
    private Path inputPath2b;
    
    @Autowired
    private Path inputPath2R;
    
    @Autowired
    private Date datestamp1;
    
    @Autowired
    private UUID workflowId1;
    
    @Autowired
    private UUID workflowId2;
    
    @Autowired
    private Workflow workflow1;
    
    @Autowired
    private Workflow workflow2;
    
    @Test
    public void test1_checkIdentifier()
    {        
        assertEquals(workflowId1, workflow1.id());
    }
    
    @Test
    public void test1_checkNodesAndDependencies()
    {   
        assertEquals(
            new HashSet<>(Arrays.asList("alpha", "alpha-validator", "bravo", "charlie", "xray")),
            workflow1.nodeNames());
        
        Workflow.JobNode nodeA = workflow1.node("alpha"), 
            nodeA1 = workflow1.node("alpha-validator"),
            nodeB = workflow1.node("bravo"), 
            nodeC = workflow1.node("charlie"), 
            nodeX = workflow1.node("xray");
        
        assertEquals(nodeA.name(), "alpha");
        assertEquals(nodeA1.name(), "alpha-validator");
        assertEquals(nodeB.name(), "bravo");
        assertEquals(nodeC.name(), "charlie");
        assertEquals(nodeX.name(), "xray");
        
        System.err.println(workflow1.debugGraph());
        
        // Test dependencies
        
        assertEquals(Collections.emptySet(), new HashSet<>(toList(nodeA.dependencies())));
        assertEquals(new HashSet<>(Arrays.asList(nodeA1, nodeB, nodeC)), new HashSet<>(toList(nodeA.dependents())));

        assertEquals(Collections.singleton(nodeA), new HashSet<>(toList(nodeA1.dependencies())));
        assertEquals(new HashSet<>(Arrays.asList(nodeB, nodeC)), new HashSet<>(toList(nodeA1.dependents())));
        
        assertEquals(new HashSet<>(Arrays.asList(nodeA, nodeA1)), new HashSet<>(toList(nodeB.dependencies())));
        assertEquals(Collections.emptySet(), new HashSet<>(toList(nodeB.dependents())));
        
        assertEquals(new HashSet<>(Arrays.asList(nodeA, nodeA1)), new HashSet<>(toList(nodeC.dependencies())));
        assertEquals(Collections.emptySet(), new HashSet<>(toList(nodeC.dependents())));
        
        assertEquals(Collections.emptySet(), new HashSet<>(toList(nodeX.dependencies())));
        assertEquals(Collections.emptySet(), new HashSet<>(toList(nodeX.dependents())));
        
        // Test inputs and outputs for nodes
       
        final String defaultInputKey = Workflow.DEFAULT_INPUT_KEY;
        
        assertEquals(Collections.singleton(defaultInputKey), nodeA.inputMap().keySet());
        assertEquals(nodeA.input(), nodeA.inputMap().get(defaultInputKey));
        
        assertEquals(Collections.singleton(defaultInputKey), nodeA1.inputMap().keySet());
        assertEquals(nodeA1.input(), nodeA1.inputMap().get(defaultInputKey));
        
        assertEquals(Collections.singleton(defaultInputKey), nodeB.inputMap().keySet());
        assertEquals(nodeB.input(), nodeB.inputMap().get(defaultInputKey));
        
        assertEquals(Collections.singleton(defaultInputKey), nodeC.inputMap().keySet());
        assertEquals(nodeC.input(), nodeC.inputMap().get(defaultInputKey));
        
        assertEquals(Collections.emptySet(), nodeX.inputMap().keySet());
        
        for (Workflow.JobNode node: workflow1.nodes()) {
            Path stagingDir = workflow1.stagingDirectory(node.name());
            assertTrue("Expected output of node to be inside its own staging directory", 
                IterableUtils.matchesAll(node.output(), p -> p.startsWith(stagingDir)));
        }
        
        assertEquals(Collections.singletonList(inputPath1), nodeA.input());
        
        assertTrue("Expected input of `alpha-validator` to contain output of `alpha`", 
            nodeA1.input().containsAll(nodeA.output()));
        
        assertTrue("Expected input of `bravo` to contain output of `alpha`", 
            nodeB.input().containsAll(nodeA.output()));
        
        assertTrue("Expected input of `charlie` to contain output of `alpha` named a1.txt", 
            nodeC.input().containsAll(
                ListUtils.select(nodeA.output(), p -> p.getFileName().toString().equals("a1.txt"))));
        
        assertEquals(Collections.emptyList(), nodeX.input());
        
        // Test job-name and flow, and parameters
     
        for (Workflow.JobNode node: workflow1.nodes()) {
            final JobParameters parameters = node.parameters();
            final String jobName = node.jobName();
            final Flow flow = node.flow(stepBuilderFactory);
            assertNotNull("A node is expected to have a job name", jobName);
            assertNotNull("A node is expected to associate to a Batch flow", flow);
            assertNotNull("Expected a parameter fpr the workflow identifier", 
                parameters.getString(Workflow.WORKFLOW_PARAMETER_NAME));
            final String inputAsString = parameters.getString(Workflow.INPUT_PARAMETER_NAME, "");
            List<Path> inputs = Arrays.stream(inputAsString.split(File.pathSeparator))
                .filter(s -> !s.isEmpty())
                .collect(Collectors.mapping(Paths::get, Collectors.toList()));
            assertEquals(inputs, node.input());
            assertTrue(inputs.stream().allMatch(Path::isAbsolute));
        }
    }
    
    @Test
    public void test1_checkParameters()
    {
        Workflow.JobNode nodeA = workflow1.node("alpha"), 
            nodeA1 = workflow1.node("alpha-validator"),
            nodeB = workflow1.node("bravo"), 
            nodeC = workflow1.node("charlie"), 
            nodeX = workflow1.node("xray");
        
        assertEquals(nodeA.parameters().getString("greeting"), "Hello World");
        assertEquals(nodeA1.parameters().getString("strict"), "true");
        assertEquals(nodeA.parameters().getLong("number"), Long.valueOf(199L));
        assertEquals(nodeB.parameters().getDate("now"), datestamp1);
        assertEquals(nodeC.parameters().getDouble("pi"), Double.valueOf(Math.PI));
        assertEquals(nodeX.parameters().getLong("magic"), Long.valueOf(1997));
    }
    
    @Test(expected = IllegalArgumentException.class) 
    public void test1_failIfNameNotExists()
    {
        workflow1.node("zulu");
    }
    
    @Test(expected = UnsupportedOperationException.class) 
    public void test1_checkUnmodifiableNames()
    {
        workflow1.nodeNames().add("zulu");
    }
    
    @Test(expected = UnsupportedOperationException.class) 
    public void test1_checkUnmodifiableOutputMapOfPaths()
    {
        workflow1.output().put("foo.txt", Paths.get("baz.txt"));
    }
    
    @Test(expected = UnsupportedOperationException.class) 
    public void test1_checkUnmodifiableOutputMapOfUris()
    {
        workflow1.outputUris().put("foo.txt", URI.create("file:///tmp/baz.txt"));
    }
    
    @Test(expected = UnsupportedOperationException.class) 
    public void test1_checkUnmodifiableInputMapA1()
    {
        workflow1.node("alpha").inputMap()
            .put("foo", Collections.singletonList(Paths.get("foo-1.txt")));
    }
    
    @Test(expected = UnsupportedOperationException.class) 
    public void test1_checkUnmodifiableInputMapA2()
    {
        final String defaultInputKey = Workflow.DEFAULT_INPUT_KEY;
        workflow1.node("alpha").inputMap()
            .put(defaultInputKey, Collections.singletonList(Paths.get("other.txt")));
    }
    
    @Test(expected = UnsupportedOperationException.class) 
    public void test1_checkUnmodifiableInputMapA3()
    {
        final String defaultInputKey = Workflow.DEFAULT_INPUT_KEY;
        workflow1.node("alpha").inputMap().remove(defaultInputKey);
    }
    
    @Test(expected = UnsupportedOperationException.class) 
    public void test1_checkUnmodifiableInputMapA4()
    {
        final String defaultInputKey = Workflow.DEFAULT_INPUT_KEY;
        workflow1.node("alpha").inputMap()
            .get(defaultInputKey).add(Paths.get("more.txt"));
    }
    
    @Test(expected = UnsupportedOperationException.class) 
    public void test1_checkUnmodifiableInputMapA5()
    {
        final String defaultInputKey = Workflow.DEFAULT_INPUT_KEY;
        workflow1.node("alpha").inputMap()
            .get(defaultInputKey).set(0, Paths.get("another.txt"));
    }
    
    @Test(expected = UnsupportedOperationException.class) 
    public void test1_checkUnmodifiableInputMapA6()
    {
        final String defaultInputKey = Workflow.DEFAULT_INPUT_KEY;
        workflow1.node("alpha").inputMap()
            .get(defaultInputKey).clear();
    }
    
    @Test
    public void test1_checkNodesInTopologicalOrder()
    {
        List<String> names = IterableUtils.toList(
            IterableUtils.transformedIterable(
                workflow1.nodesInTopologicalOrder(), y -> y.name()));
        
        assertTrue("Expected node `alpha` to precede node `alpha-validator`", 
            names.indexOf("alpha") < names.indexOf("alpha-validator"));
        assertTrue("Expected node `alpha` to precede node `bravo`", 
            names.indexOf("alpha") < names.indexOf("bravo"));
        assertTrue("Expected node `alpha-validator` to precede node `bravo`", 
            names.indexOf("alpha-validator") < names.indexOf("bravo"));
        assertTrue("Expected node `alpha` to precede node `charlie`", 
            names.indexOf("alpha") < names.indexOf("charlie"));
        assertTrue("Expected node `alpha-validator` to precede node `charlie`", 
            names.indexOf("alpha-validator") < names.indexOf("charlie"));
    }
    
    @Test
    public void test1_checkOutputMap()
    {
        Map<String,Path> outputMap = workflow1.output();
        
        assertEquals(workflow1.outputNames(), outputMap.keySet());
        
        assertEquals(
            new HashSet<>(Arrays.asList("res-b-1.txt", "res-c-1.txt")),
            outputMap.keySet());
        assertTrue(
            outputMap.get("res-b-1.txt").startsWith(workflow1.stagingDirectory("bravo")));
        assertTrue(
            outputMap.get("res-b-1.txt").endsWith("b1.txt"));
        assertTrue(
            outputMap.get("res-c-1.txt").startsWith(workflow1.stagingDirectory("charlie")));
        assertTrue(
            outputMap.get("res-c-1.txt").endsWith("c1.txt"));
    }
    
    @Test
    public void test1_checkPaths()
    {
        Path workflowDir = workflow1.dataDirectory();
        Path workflowOutputDir = workflow1.outputDirectory();
        
        assertTrue("Expected an absolute path", dataDir.isAbsolute());
        assertTrue("Expected an absolute path", workflowDir.isAbsolute());
        
        assertTrue("Expected workflow\'s data directory inside given data directory", 
            workflowDir.startsWith(dataDir));
        assertTrue("Expected output directory inside workflow\'s data directory", 
            workflowOutputDir.startsWith(workflowDir));
        
        for (String nodeName: workflow1.nodeNames()) {
            assertTrue("Expected node\'s staging directory inside workflow\'s data directory", 
                workflow1.stagingDirectory(nodeName).startsWith(workflowDir));
        }
    }

    @Test
    public void test1_checkListeners()
    {
        assertTrue(workflow1.getListeners().size() == 1);
        assertTrue(workflow1.getListeners("alpha").size() == 1);
        assertTrue(workflow1.getListeners("alpha-validator").size() == 1);
        assertTrue(workflow1.getListeners("bravo").isEmpty());
        assertTrue(workflow1.getListeners("charlie").isEmpty());
        assertTrue(workflow1.getListeners("xray").isEmpty());
        assertTrue(workflow1.getListeners("zulu").isEmpty());
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void test1_checkUnmodifiableListeners1()
    {
        workflow1.getListeners().add(new WorkflowExecutionListener() {});
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void test1_checkUnmodifiableListeners2()
    {
        workflow1.getListeners("alpha").add(new WorkflowExecutionListener() {});
    }
    
    @Test
    public void test2_checkNodesAndDependencies()
    {
        assertEquals(
            new HashSet<>(Arrays.asList("transform-A", "transform-B-P1", "transform-B-P2", "interlink")),
            workflow2.nodeNames());
        
        Workflow.JobNode nodeA = workflow2.node("transform-A"), 
            nodeB1 = workflow2.node("transform-B-P1"),
            nodeB2 = workflow2.node("transform-B-P2"),
            nodeL = workflow2.node("interlink");
        
        assertEquals(nodeA.name(), "transform-A");
        assertEquals(nodeB1.name(), "transform-B-P1");
        assertEquals(nodeB2.name(), "transform-B-P2");
        assertEquals(nodeL.name(), "interlink");
        
        System.err.println(workflow2.debugGraph());
        
        // Test dependencies
        
        assertEquals(Collections.emptySet(), new HashSet<>(toList(nodeA.dependencies())));
        assertEquals(Collections.singleton(nodeL), new HashSet<>(toList(nodeA.dependents())));
        
        assertEquals(Collections.emptySet(), new HashSet<>(toList(nodeB1.dependencies())));
        assertEquals(Collections.singleton(nodeB2), new HashSet<>(toList(nodeB1.dependents())));
        
        assertEquals(Collections.singleton(nodeB1), new HashSet<>(toList(nodeB2.dependencies())));
        assertEquals(Collections.singleton(nodeL), new HashSet<>(toList(nodeB2.dependents())));
        
        assertEquals(new HashSet<>(Arrays.asList(nodeA, nodeB2)), new HashSet<>(toList(nodeL.dependencies())));
        assertEquals(Collections.emptySet(), new HashSet<>(toList(nodeL.dependents())));
        
        // Test inputs and outputs for nodes
        
        final String defaultInputKey = Workflow.DEFAULT_INPUT_KEY;
        
        assertEquals(Collections.singleton(defaultInputKey), nodeA.inputMap().keySet());
        assertEquals(nodeA.input(), nodeA.inputMap().get(defaultInputKey));
        
        assertEquals(Collections.singleton(defaultInputKey), nodeB1.inputMap().keySet());
        assertEquals(nodeB1.input(), nodeB1.inputMap().get(defaultInputKey));
        
        assertEquals(Collections.singleton(defaultInputKey), nodeB2.inputMap().keySet());
        assertEquals(nodeB2.input(), nodeB2.inputMap().get(defaultInputKey));
        
        assertEquals(new HashSet<>(Arrays.asList("A", "B", "rules")), nodeL.inputMap().keySet());
        assertEquals(nodeL.input().size(), nodeL.inputMap().get("A").size() + 
            nodeL.inputMap().get("B").size() + nodeL.inputMap().get("rules").size());
        assertTrue(nodeL.input().containsAll(nodeL.inputMap().get("A")));
        assertTrue(nodeL.input().containsAll(nodeL.inputMap().get("B")));
        assertTrue(nodeL.input().containsAll(nodeL.inputMap().get("rules")));
        
        // Test parameters
        
        final String workflowParameterName = Workflow.WORKFLOW_PARAMETER_NAME;
        final String inputParameterName = Workflow.INPUT_PARAMETER_NAME;
        
        assertEquals(
            new HashSet<>(Arrays.asList("a1", workflowParameterName, inputParameterName)),
            nodeA.parameters().getParameters().keySet());
        assertEquals(inputPath2a.toString(), nodeA.parameters().getString(inputParameterName));
        
        assertEquals(
            new HashSet<>(Arrays.asList("b11", workflowParameterName, inputParameterName)),
            nodeB1.parameters().getParameters().keySet());
        assertEquals(inputPath2b.toString(), nodeB1.parameters().getString(inputParameterName));
        
        assertEquals(
            new HashSet<>(Arrays.asList("b21", workflowParameterName, inputParameterName)),
            nodeB2.parameters().getParameters().keySet());
        assertTrue(nodeB2.parameters().getString(inputParameterName)
            .startsWith(workflow2.stagingDirectory("transform-B-P1").toString()));
        
        assertEquals(
            new HashSet<>(Arrays.asList(
                "m1", "threshold",
                workflowParameterName, 
                inputParameterName + "." + "A", 
                inputParameterName + "." + "B", 
                inputParameterName + "." + "rules")),
            nodeL.parameters().getParameters().keySet());
        
        assertTrue(nodeL.parameters().getString(inputParameterName + "." + "A")
            .startsWith(workflow2.stagingDirectory("transform-A").toString()));
        assertTrue(nodeL.parameters().getString(inputParameterName + "." + "B")
            .startsWith(workflow2.stagingDirectory("transform-B-P2").toString()));
        assertEquals(inputPath2R.toString(), nodeL.parameters().getString(inputParameterName + "." + "rules"));
    }
    
    @Test
    public void test2_checkNodesInTopologicalOrder()
    {
        List<String> names = IterableUtils.toList(
            IterableUtils.transformedIterable(
                workflow2.nodesInTopologicalOrder(), y -> y.name()));
        
        assertTrue("Expected node `transform-A` to precede node `interlink`", 
            names.indexOf("transform-A") < names.indexOf("interlink"));
        
        assertTrue("Expected node `transform-B1` to precede node `transform-B-P2`", 
            names.indexOf("transform-B-P1") < names.indexOf("transform-B-P2"));
        
        assertTrue("Expected node `transform-B-P2` to precede node `interlink`", 
            names.indexOf("transform-B-P2") < names.indexOf("interlink"));
    }
}
