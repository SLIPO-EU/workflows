package eu.slipo.workflows;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.collections4.map.Flat3Map;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.util.AntPathMatcher;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import eu.slipo.workflows.tasklet.CopyOutputTasklet;
import eu.slipo.workflows.tasklet.ExportExecutionContextTasklet;
import eu.slipo.workflows.tasklet.ImportExecutionContextTasklet;
import eu.slipo.workflows.util.digraph.DependencyGraph;
import eu.slipo.workflows.util.digraph.DependencyGraphs;
import eu.slipo.workflows.util.digraph.ExportDependencyGraph;
import eu.slipo.workflows.util.digraph.TopologicalSort.CycleDetected;

public class Workflow
{
    /**
     * A name prefix for names of jobs created from a workflow
     */
    public static final String JOB_NAME_PREFIX = "workflow";
    
    /**
     * The output directory relative to a workflow's data directory
     */
    public static final String OUTPUT_DIR = "output";
    
    /**
     * The (reserved) name for a parameter holding a colon-separated list of input paths.
     */
    public static final String INPUT_PARAMETER_NAME = "input";
    
    /**
     * The (reserved) name for a parameter holding the UUID of the workflow that a job 
     * is part of.
     */
    public static final String WORKFLOW_PARAMETER_NAME = "workflow";
    
    /**
     * The default key for an input of a job node (representing an anonymous input).
     */
    public static final String DEFAULT_INPUT_KEY = ".";

    /**
     * Represent a result produced by a job of a workflow.
     */
    static class Result
    {
        /**
         * The URI scheme for URIs that represent internal results of a workflow
         */
        protected static final String SCHEME = "res";
        
        /**
         * An empty path that stands for the current directory  
         */
        protected static final Path DOT_PATH = Paths.get("");
        
        /**
         * The name of a job node expected to produce this result.
         */
        private final String nodeName;
        
        /**
         * The path under which this result is (expected to be) created. This path 
         * should be relative to node's staging output directory.
         * 
         * <p>Note that this path can be empty or a dot (".") path; in such a case it simply
         * declares a dependency to the named job node. 
         */
        private final Path outputPath;

        /**
         * A key that locates an entry inside the output file. This is meaningfull only for 
         * output JSON files containing key-value entries (and is only used for accessing parts
         * of exported execution context).
         */
        private final String key;
        
        private Result(String nodeName, Path outputPath, String key)
        {
            Assert.isTrue(!StringUtils.isEmpty(nodeName), "Expected a non-empty name for a job node");
            Assert.notNull(outputPath, "An output path is required");
            Assert.isTrue(!outputPath.isAbsolute(), "Expected a relative output path");
            this.nodeName = nodeName;
            this.outputPath = outputPath.normalize();
            this.key = key;
        }
        
        /**
         * The name of a job node expected to produce this result.
         */
        public String nodeName()
        {
            return nodeName;
        }
        
        /**
         * The output path relative to node's staging output directory
         */
        public Path outputPath()
        {
            return outputPath;
        }
        
        /**
         * The output path relative to data directory of a workflow 
         */
        public Path path()
        {
            Path parent = parentPath();
            return parent.resolve(outputPath);
        }
        
        /**
         * The key that locates an entry inside the output file; will be <tt>null</tt> if a key is
         * irrelevant to the kind of output this result represents.
         */
        public String key()
        {
            return key;
        }
        
        /**
         * The output directory relative to data directory of a workflow. This is the parent 
         * directory of the expected output.
         */
        public Path parentPath()
        {
            return Paths.get("stage", nodeName, "output");
        }
        
        public static Result of(String nodeName)
        {
            return new Result(nodeName, DOT_PATH, null);
        }
        
        public static Result of(String nodeName, Path outputPath)
        {
            return new Result(nodeName, outputPath, null);
        }
        
        public static Result of(String nodeName, String outputPath)
        {
            return new Result(nodeName, Paths.get(outputPath), null);
        }
        
        public static Result of(String nodeName, Path outputPath, String entryKey)
        {
            return new Result(nodeName, outputPath, entryKey);
        }
        
        public static Result of(String nodeName, String outputPath, String entryKey)
        {
            return new Result(nodeName, Paths.get(outputPath), entryKey);
        }
        
        public static Result of(URI uri)
        {
            Assert.isTrue(uri.getScheme().equals(SCHEME), 
                "The given URI is malformed (does not begin with the expected scheme)");
            String nodeName = uri.getHost();
            Assert.isTrue(!StringUtils.isEmpty(nodeName), 
                "The host part (i.e the node name) cannot be empty");
            String path = uri.getPath().replaceFirst("/", "");
            String key = uri.getFragment();
            return new Result(nodeName, Paths.get(path), key);
        }
        
        public static Result parse(URI uri)
        {
            if (!uri.getScheme().equals(SCHEME))
                return null;
            
            String nodeName = uri.getHost();
            String path = uri.getPath().replaceFirst("/", "");
            String key = uri.getFragment();
            return new Result(nodeName, Paths.get(path), key);
        }
        
        public URI toUri()
        {
            URI uri = null;
            try {
                uri = new URI(SCHEME, nodeName, "/" + outputPath.toString(), key);
            } catch (URISyntaxException ex) {
                throw new IllegalStateException(ex);
            }
            return uri;
        }
        
        @Override
        public String toString()
        {
            return toUri().toString();
        }
    }
     
    /**
     * Represent the configuration of a job participating in a workflow. 
     */
    public static class JobDefinition
    {
        /**
         * A name (unique for a given workflow) for this job node.
         */
        private String name;
        
        /**
         * The actual flow to be wrapped as a job node of a workflow. 
         */
        private Flow flow;
        
        /**
         * A map of parameters to be forwarded to the job.
         */
        private JobParameters parameters;
        
        /** 
         * A map of inputs (keyed to a group) for this job.
         * 
         * <p>These inputs are given as URIs in one of the following formats:
         * <ul>
         *   <li><tt>file:///&lt;path&gt;</tt>: an external (to parent workflow) file supplied 
         *     as input</li>
         *   <li><tt>res://&lt;dependency-name&gt;/&lt;input-path&gt;</tt>: a result produced by another 
         *     job of the same workflow.</li>
         * </ul>
         * 
         *  @see {@link Result}
         */
        private Map<String, List<URI>> inputMap;
        
        /**
         * A list of output files (relative to this job's output directory) that should be copied
         * to the workflow-level output directory.
         */
        private List<Path> output;
        
        /**
         * A set of keys that represent context entries to be exported (to other nodes)
         */
        private Set<String> exportedKeys;

        /**
         * A map of keys that refer to context entries imported from other nodes. 
         * 
         * <p>Note that each context entry is represented by a URI of {@code res://<node-name>/<path>#<contextKey>} 
         */
        private Map<String, URI> contextMap;
        
        private JobDefinition(String name, Flow flow) 
        {
            this.name = name;
            this.flow = flow;
        }

        private JobDefinition withInput(Map<String, List<URI>> input)
        {
            JobDefinition def = new JobDefinition(this.name, this.flow);
            def.inputMap = input;
            def.output = this.output;
            def.parameters = this.parameters;
            def.exportedKeys = this.exportedKeys;
            def.contextMap = this.contextMap;
            return def;
        }
       
        private JobDefinition withOutput(List<Path> output)
        {
            JobDefinition def = new JobDefinition(this.name, this.flow);
            def.inputMap = this.inputMap;
            def.output = output;
            def.parameters = this.parameters;
            def.exportedKeys = this.exportedKeys;
            def.contextMap = this.contextMap;
            return def;
        }
        
        public String name()
        {
            return name;
        }
        
        public String nodeName()
        {
            return name;
        }

        public Flow flow()
        {
            return flow;
        }

        public JobParameters parameters()
        {
            return parameters;
        }

        public Map<String, List<URI>> inputMap()
        {
            return inputMap;
        }
 
        public List<URI> input()
        {
            return inputMap.values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
        }
        
        public List<Path> output()
        {
            return output;
        }
        
        public Set<String> exportedKeys()
        {
            return exportedKeys;
        }
        
        public Map<String, URI> contextMap()
        {
            return contextMap;
        }

        @Override
        public String toString()
        {
            return String.format(
                "JobDefinition [name=%s, input=%s, output=%s]", name, inputMap, output);
        }
    }
    
    /**
     * A builder for a {@link JobDefinition}.  
     */
    public static class JobDefinitionBuilder
    {
        /**
         * The pattern that a name part must conform to. A name may consist of several 
         * dot-delimited parts (e.g. "aggregator.q1").
         */
        public static final Pattern namePartPattern = Pattern.compile("^[a-zA-Z][-\\w]*$");
        
        /**
         * The pattern that an input key must conform to.
         */
        public static final Pattern inputKeyPattern = Pattern.compile("^[a-zA-Z][\\w]*$");
        
        private String name;
        
        private Flow flow;
        
        private JobParameters parameters;
        
        private Map<String, Set<URI>> inputMap = new HashMap<>();
        
        private List<Path> output = new ArrayList<>();
        
        private List<String> exportedKeys = new ArrayList<>();
        
        private Map<String, URI> contextMap = new HashMap<>();
        
        public static JobDefinitionBuilder create(String name)
        {
            JobDefinitionBuilder builder = new JobDefinitionBuilder();
            return builder.name(name);
        }
        
        public JobDefinitionBuilder() {}
                
        /**
         * Name this job. This is the name of the job node and should be unique across 
         * a given workflow.
         * 
         * <p>Note: This name is, in general, different from the name with which a job is 
         * launched as an actual Batch job (and which is decided by a {@link WorkflowScheduler}).
         * 
         * @param name The name of a job node
         */
        public JobDefinitionBuilder name(String name)
        {
            Assert.isTrue(!StringUtils.isEmpty(name), "Expected a name for this job");
            
            String[] nameParts = name.split(Pattern.quote("."));
            Assert.isTrue(Arrays.stream(nameParts)
                    .allMatch(part -> namePartPattern.matcher(part).matches()), 
                "The given name is invalid");
            this.name = name;
            return this;
        }
        
        /**
         * Provide the flow to be executed as the trunk of the job. 
         * 
         * @param flow
         */
        public JobDefinitionBuilder flow(Flow flow)
        {
            Assert.notNull(flow, "Expected a non-null flow");
            this.flow = flow;
            return this;
        }
        
        /**
         * Provide a single step to be executed as the trunk of the job.
         * 
         * <p>This is convenience method that simply wraps a {@link Step} into a {@link Flow}.
         * @param flow
         */
        public JobDefinitionBuilder flow(Step step)
        {
            Assert.notNull(step, "Expected a non-null step");
            this.flow = new FlowBuilder<Flow>(step.getName()).start(step).end();
            return this;
        }
        
        /**
         * Provide a map of parameters for the job. These parameters will be forwarded
         * to the actual run of a Batch job.
         * 
         * @param parameters
         */
        public JobDefinitionBuilder parameters(JobParameters parameters)
        {
            Assert.notNull(parameters, "Expected a non-null map of parameters");
            this.parameters = parameters;
            return this;
        }
        
        /**
         * Provide a map of parameters for the job. 
         * <p>This is a lambda-friendly version of {@link JobDefinitionBuilder#parameters(JobParameters)}
         * 
         * @param defaultParameters A map of default parameters to start from (may be null).
         * @param configurer A function to configure a {@link JobParametersBuilder}
         */
        public JobDefinitionBuilder parameters(
            JobParameters defaultParameters, Consumer<JobParametersBuilder> configurer)
        {
            Assert.notNull(configurer, "Expected a non-null configurer function");
            JobParametersBuilder parametersBuilder = defaultParameters == null?
                new JobParametersBuilder() : new JobParametersBuilder(defaultParameters);
            configurer.accept(parametersBuilder);
            this.parameters = parametersBuilder.toJobParameters();
            return this;
        }
        
        /**
         * Provide a map of parameters for the job.
         * @see {@link JobDefinitionBuilder#parameters(JobParameters, Consumer)}
         * 
         * @param configurer A function to configure a {@link JobParametersBuilder}
         */
        public JobDefinitionBuilder parameters(Consumer<JobParametersBuilder> configurer)
        {
            return parameters(null, configurer);
        }
        
        /**
         * Provide a map of parameters for the job.
         * 
         * <p>This is a convenience wrapper around {@link JobDefinitionBuilder#parameters(JobParameters)} 
         * to convert a map to a {@link JobParameters} instance, while preserving the basic value types. 
         * 
         * @param parameters A map of parameters
         */
        public JobDefinitionBuilder parameters(Map<?,?> parameters)
        {
            final JobParametersBuilder parametersBuilder = new JobParametersBuilder();
            parameters.forEach((name, value) -> {
                final String key = name.toString();
                if (value instanceof Date)
                    parametersBuilder.addDate(key, (Date) value);
                else if (value instanceof Double)
                    parametersBuilder.addDouble(key, (Double) value);
                else if (value instanceof Number)
                    parametersBuilder.addLong(key, ((Number) value).longValue());
                else
                    parametersBuilder.addString(key, value.toString());
            });
            return parameters(parametersBuilder.toJobParameters());
        }
        
        /**
         * Add another input file (external to a workflow).
         * 
         * @param path An absolute file path
         */
        public JobDefinitionBuilder input(Path path)
        {
            Assert.isTrue(path != null && path.isAbsolute(), "Expected a non-null absolute file path");
            this.addInput(DEFAULT_INPUT_KEY, path.toUri());
            return this;
        }
        
        /**
         * Add another input file (external to a workflow). The input will be logically grouped under 
         * a given input key.
         * 
         * @param path An absolute file path
         * @param inputKey
         */
        public JobDefinitionBuilder input(Path path, String inputKey)
        {
            Assert.isTrue(path != null && path.isAbsolute(), "Expected a non-null absolute file path");
            Assert.isTrue(!StringUtils.isEmpty(inputKey), "Expected a non-empty input key");
            Assert.isTrue(inputKeyPattern.matcher(inputKey).matches(), "The input key is invalid");
            this.addInput(inputKey, path.toUri());
            return this;
        }
        
        /**
         * Add another input file as an expected (i.e. promised) result from a job we depend on.
         * 
         * @param dependencyName The name of the job node that we depend on (and must precede)
         * @param path A path that will be resolved relative to the output directory
         *   of the dependency. An empty path (or a "." path) can also be used to just declare a 
         *   dependency (in the meaning of precedence). A glob-style path can also be used here and 
         *   will be expanded to matching paths when the workflow is built.
         */
        public JobDefinitionBuilder input(String dependencyName, Path path)
        {
            Assert.notNull(dependencyName, "Expected a non-null name for a job node");
            Assert.isTrue(path != null && !path.isAbsolute(), 
                "Expected a non-null node-relative path for a result");
            Result result = Result.of(dependencyName, path);
            this.addInput(DEFAULT_INPUT_KEY, result.toUri());
            return this;
        }
        
        public JobDefinitionBuilder input(String dependencyName, String path)
        {
            return this.input(dependencyName, Paths.get(path));
        }
        
        /**
         * Add another input file as an expected (i.e. promised) result from a job we depend on. The input
         * will be logically grouped under a given input key.
         * 
         * @param dependencyName The name of the job node that we depend on (and must precede)
         * @param path A path that will be resolved relative to the output directory of the
         *   dependency.
         * @param inputKey
         * 
         * @see JobDefinitionBuilder#input(String, Path)
         */
        public JobDefinitionBuilder input(String dependencyName, Path path, String inputKey)
        {
            Assert.notNull(dependencyName, "Expected a non-null name for a job node");
            Assert.isTrue(path != null && !path.isAbsolute(), 
                "Expected a non-null node-relative path for a result");
            Assert.isTrue(!StringUtils.isEmpty(inputKey), "Expected a non-empty input key");
            Assert.isTrue(inputKeyPattern.matcher(inputKey).matches(), "The input key is invalid");
            Result result = Result.of(dependencyName, path);
            this.addInput(inputKey, result.toUri());
            return this;
        }
        
        public JobDefinitionBuilder input(String dependencyName, String path, String inputKey)
        {
            return this.input(dependencyName, Paths.get(path), inputKey);
        }
        
        /**
         * Add a bunch of dependencies having a common relative (to dependency) path.
         * 
         * <p>This is just a convenience method that iterates on given dependencies adding
         * each pair of (dependency, path) to our input specification. 
         *   
         * @param dependencyNames A collection of dependencies
         * @param path The common dependency-relative path
         */
        public JobDefinitionBuilder input(Collection<String> dependencyNames, Path path)
        {
            for (String dependencyName: dependencyNames)
                input(dependencyName, path);
            return this;
        }
        
        public JobDefinitionBuilder input(Collection<String> dependencyNames, String path)
        {
            return this.input(dependencyNames, Paths.get(path));
        }
        
        /**
         * @see JobDefinitionBuilder#input(Collection, Path)
         */
        public JobDefinitionBuilder input(Collection<String> dependencyNames, Path path, String inputKey)
        {
            for (String dependencyName: dependencyNames)
                input(dependencyName, path, inputKey);
            return this;
        }
        
        public JobDefinitionBuilder input(Collection<String> dependencyNames, String path, String inputKey)
        {
            return this.input(dependencyNames, Paths.get(path), inputKey);
        }
        
        private void addInput(String inputKey, URI uri)
        {
            this.inputMap.computeIfAbsent(inputKey, k -> new LinkedHashSet<>())
                .add(uri);
        }
        
        /**
         * Declare an explicit dependency to another job node.
         * 
         * <p>This is convenience method wrapping <tt>input</tt> method provided by this builder;
         * one could use the {@link JobDefinitionBuilder#input(String, Path)} method passing an 
         * empty or a dot (".") path as the <tt>outputPath</tt>.
         * 
         * @param dependencyName The name of the job node that we depend on (and must precede)
         */
        public JobDefinitionBuilder after(String dependencyName)
        {
            return input(dependencyName, Paths.get("."));
        }
        
        /**
         * Designate a file as an the output of this job.
         * <p>An output of a job is made accessible to other parts of the same workflow
         * by making a copy (or link) of it inside a staging output directory.
         * 
         * @param path A path relative to the output directory of the job
         */
        public JobDefinitionBuilder output(Path path)
        {   
            Assert.isTrue(path != null && !path.isAbsolute(), "Expected a non-empty relative file path");
            this.output.add(path);
            return this;
        }
        
        /**
         * @see {@link JobDefinitionBuilder#output(Collection))}
         */
        public JobDefinitionBuilder output(Path ...paths)
        {
            return output(Arrays.asList(paths));
        }
        
        /**
         * Designate a collection of files as an output of this job.
         * @see {@link JobDefinitionBuilder#output(Path)}
         * 
         * @param paths A collection of paths, relative to the output directory of the job
         */
        public JobDefinitionBuilder output(Collection<Path> paths)
        {
            Assert.isTrue(paths.stream().allMatch(p -> p != null && !p.isAbsolute()), 
                "Expected a non-empty relative file path");
            this.output.addAll(paths);
            return this;
        }
        
        /**
         * @see {@link JobDefinitionBuilder#output(Collection))}
         */
        public JobDefinitionBuilder output(String ...paths)
        {
            Stream<Path> s = Arrays.stream(paths).map(Paths::get);
            return output(s.collect(Collectors.toList()));
        }
        
        public JobDefinitionBuilder output(String path)
        {
            return output(Paths.get(path));
        }
        
        /**
         * Designate a set of execution-context entries to be exported. The exported enties are available
         * to be imported by other (dependent) job nodes of the parent workflow.
         * 
         * @param keys The keys for entries to be exported
         */
        public JobDefinitionBuilder exportToContext(String ...keys)
        {
            Assert.isTrue(keys.length > 0, "Expected a non-empty array of keys");
            Assert.isTrue(Arrays.stream(keys).noneMatch(StringUtils::isEmpty), 
                "A key ie expected as a non-blank string");
            this.exportedKeys.addAll(Arrays.asList(keys));
            return this;
        }
        
        /**
         * Add an context entry importing it from the execution context of another job node.
         * 
         * <p>Note that this implies that our node will depend on the exporter node.
         * 
         * @param dependencyName The node name that exports the entry
         * @param key The key of the imported entry (as exported from the node we depend on)
         * @param toKey The key under which the entry is imported into our context
         */
        public JobDefinitionBuilder contextFrom(String dependencyName, String key, String toKey)
        {
            Assert.isTrue(!StringUtils.isEmpty(dependencyName), "Expected the name of a job node");
            Assert.isTrue(!StringUtils.isEmpty(key), 
                "Expected a key for the imported entry");
            Assert.isTrue(!StringUtils.isEmpty(toKey), 
                "Expected a key for the entry to be imported into our context");
            Result result = Result.of(dependencyName, "context.json", key);
            this.contextMap.put(toKey, result.toUri());
            return this.after(dependencyName);
        }
        
        public JobDefinition build()
        {
            Assert.state(!StringUtils.isEmpty(this.name), "A non-empty name is required for a job node");
            Assert.state(this.flow != null, "A trunk flow is required for a job");
                
            JobDefinition def = new JobDefinition(this.name, this.flow);
            
            final Map<String, List<URI>> input = 
                (this.inputMap.size() > 3)? (new HashMap<>()) : (new Flat3Map<>()); 
            this.inputMap.forEach((inputKey, uris) -> {
                input.put(inputKey, new ArrayList<>(uris));
            });
            def.inputMap = Collections.unmodifiableMap(input);
            
            def.parameters = this.parameters == null? (new JobParameters()) : this.parameters;
            def.output = Collections.unmodifiableList(new ArrayList<>(this.output));
            def.exportedKeys = Collections.unmodifiableSet(new HashSet<>(this.exportedKeys));
            def.contextMap = Collections.unmodifiableMap(new HashMap<>(this.contextMap));
            
            return def;
        }
    }
    
    /**
     * Represent a job that participates in a workflow.
     * 
     * <p>A {@link WorkflowScheduler} must map it to a single {@link JobInstance}, and
     * start it when its dependencies are complete.
     */
    public class JobNode
    {
        /**
         * The node (i.e. vertex) number for this node in parent workflow
         */
        private final int vertex;
        
        private JobNode(int vertex)
        {
            this.vertex = vertex;
        }
        
        protected int vertex()
        {
            return vertex;
        }
        
        /**
         * Get the name of the job node
         */
        public String name()
        {
            return defs.get(vertex).name();
        }
        
        /**
         * A name as a (suggestion of a) job name passed to Batch
         */
        public String jobName()
        {
            return JOB_NAME_PREFIX + "." + (defs.get(vertex).flow().getName());
        }
        
        /**
         * Build a flow to be executed inside a Batch job. 
         * 
         * <p>Note that this flow will not be the same as the one given to the original 
         * {@link JobDefinition}, as it is enhanced (by wrapping it into another flow) to 
         * support data exchange (files or key-value entries) between jobs of same workflow.
         * 
         * @param stepBuilderFactory
         */
        public Flow flow(StepBuilderFactory stepBuilderFactory)
        {
            final JobDefinition def = defs.get(vertex);
            final Flow trunkFlow = def.flow();
            
            final String flowName = JOB_NAME_PREFIX + "." + trunkFlow.getName();
            FlowBuilder<Flow> flowBuilder = new FlowBuilder<Flow>(flowName);
            
            final Map<String, URI> contextMap = contextMap();
            if (!contextMap.isEmpty()) {
                Step importContextStep = stepBuilderFactory.get(JOB_NAME_PREFIX + ".importContext")
                    .tasklet(new ImportExecutionContextTasklet(contextMap))
                    .listener(ExecutionContextPromotionListeners.fromKeys(contextMap.keySet()))
                    .build();
                flowBuilder = flowBuilder.start(importContextStep).next(trunkFlow);
            } else {
                flowBuilder = flowBuilder.start(trunkFlow);
            }
            
            final List<Path> outputPaths = def.output();
            if (!outputPaths.isEmpty()) {
                // Append a step to copy output into node's staging directory
                Path targetDir = outputDir();
                Step copyOutputStep = stepBuilderFactory.get(JOB_NAME_PREFIX + ".copyOutput")
                    .tasklet(new CopyOutputTasklet(targetDir, outputPaths))
                    .build();
                flowBuilder = flowBuilder.next(copyOutputStep);
            } 
            
            final Set<String> exportedKeys = def.exportedKeys();
            if (!exportedKeys.isEmpty()) {
                Path targetPath = exportedContextPath();
                Step exportContextStep = stepBuilderFactory.get(JOB_NAME_PREFIX + ".exportContext")
                    .tasklet(new ExportExecutionContextTasklet(targetPath, exportedKeys))
                    .build();
                flowBuilder = flowBuilder.next(exportContextStep);
            }
            
            return flowBuilder.build();
        }
        
        /**
         * The map of parameters to be passed to a Batch job.
         * 
         * <p>More specifically, these parameters are: 
         * <ul>
         *    <li>all parameters given to original {@link JobDefinition}</li>
         *    <li>an <tt>input</tt> parameter containing a colon-separated list of inputs 
         *      (resolved to absolute filesystem paths)</li>
         *    <li>a <tt>workflow</tt> parameter containing the id of parent workflow</li>  
         * </ul>    
         */
        public JobParameters parameters()
        {
            final JobDefinition def = defs.get(vertex);
            
            final JobParametersBuilder parametersBuilder = new JobParametersBuilder(def.parameters());
            
            parametersBuilder.addString(WORKFLOW_PARAMETER_NAME, id.toString());
            
            this.inputMap().forEach((inputKey, inputPaths) -> {
                final String parameterName = inputKey.equals(DEFAULT_INPUT_KEY)?
                    INPUT_PARAMETER_NAME : (INPUT_PARAMETER_NAME + "." + inputKey);
                final String parameterValue = inputPaths.stream()
                    .map(Path::toString)
                    .collect(Collectors.joining(File.pathSeparator));
                parametersBuilder.addString(parameterName, parameterValue);
            });
            
            return parametersBuilder.toJobParameters();
        }
        
        /**
         * Get a map of our (expected) inputs as absolute filesystem paths. Each entry represents
         * a logical group of inputs.
         * 
         * <p>Note that the corresponding files may or may not exist: only when dependencies
         * are fulfilled, these files are expected to exist and to be readable.
         */
        public Map<String, List<Path>> inputMap()
        {
            final JobDefinition def = defs.get(vertex);
            return def.inputMap().entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> convertUrisToPaths(e.getValue())));
        }
        
        /**
         * List our (expected) inputs as absolute filesystem paths.
         * 
         * @see JobNode#inputMap()
         */
        public List<Path> input()
        {
            final JobDefinition def = defs.get(vertex);
            return def.inputMap().values()
                .stream()
                .map(uris -> convertUrisToPaths(uris))
                .flatMap(List::stream)
                .collect(Collectors.toList());
        }
        
        private List<Path> convertUrisToPaths(List<URI> inputUris)
        {
            return inputUris.stream()
                .map(uri -> convertUriToAbsolutePath(uri))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        }
        
        /**
         * List our (expected) outputs as absolute filesystem paths.
         */
        public List<Path> output()
        {
            final JobDefinition def = defs.get(vertex);
            final String name = def.name();
            return def.output().stream()
                .map(p -> dataDir.resolve(Result.of(name, p).path()))
                .collect(Collectors.toList());
        }
        
        /**
         * The staging output directory as an absolute path
         */
        public Path outputDir()
        {
            final JobDefinition def = defs.get(vertex);
            final Path outputDir = Result.of(def.name()).parentPath();
            return dataDir.resolve(outputDir);
        }

        /**
         * Get an iterable of our dependencies
         */
        public Iterable<JobNode> dependencies()
        {
            final Workflow workflow = Workflow.this;
            return IterableUtils.transformedIterable(
                dependencyGraph.dependencies(vertex), v -> workflow.new JobNode(v));
        }

        /**
         * Get an iterable of our dependents
         */
        public Iterable<JobNode> dependents()
        {
            final Workflow workflow = Workflow.this;
            return IterableUtils.transformedIterable(
                dependencyGraph.dependents(vertex), v -> workflow.new JobNode(v));
        }

        @Override
        public String toString()
        {
            return String.format("%s/%s", id, name());
        }

        @Override
        public int hashCode()
        {
            final int P = 31;
            int result = 1;
            result = P * result + id.hashCode();
            result = P * result + vertex;
            return result;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;
            if (o == null || !(o instanceof JobNode))
                return false;
            JobNode other = (JobNode) o;
            return (other.workflow().equals(Workflow.this) && other.vertex == vertex);
        }
        
        /**
         * Map keys of our (imported) context entries to {@code file://} URIs of our expected
         * results.
         */
        private Map<String, URI> contextMap()
        {
            final JobDefinition def = defs.get(vertex);
            
            final Function<URI, URI> uriMapper = (resultUri) -> {
                Result result = Result.of(resultUri);
                
                // Turn stage-relative path to an absolute path
                Path path = dataDir.resolve(result.path());
                
                // The context key will be the fragment of our file:// uri 
                String fragment = result.key();
                if (fragment == null)
                    throw new IllegalStateException(
                        "Expected a non-null fragment for a res:// URI representing a context entry");
                
                // Transform to a file:// uri 
                URI uri = null; 
                try {
                    uri = new URI("file", null, path.toString(), fragment);
                } catch (URISyntaxException e) {
                    throw new IllegalStateException(e);
                }
                
                return uri;
            };
            
            return def.contextMap().entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> uriMapper.apply(e.getValue())));
        }
        
        /**
         * The absolute path to JSON file holding our exported execution context 
         */
        private Path exportedContextPath()
        {
            final JobDefinition def = defs.get(vertex);
            final Path path = Result.of(def.name(), "context.json").path();
            return dataDir.resolve(path);
        }
        
        private Workflow workflow()
        {
            return Workflow.this;
        }
    }

    /**
     * The default listener key is for listening to events from the entire workflow
     * (i.e. not a specific job node). This key should not conflict with any legal node name.
     */
    private static final String DEFAULT_LISTENER_KEY = ".";
    
    /**
     * A builder for a {@link Workflow}
     */
    public static class Builder
    { 
        private static final AntPathMatcher pathMatcher = new AntPathMatcher(); 
        
        private final Path dataDir;
        
        private final UUID id;
        
        private List<JobDefinition> defs = new ArrayList<>();
        
        /**
         * Map output filenames (i.e. workflow results) to intermediate result URIs.
         */
        private Map<String,URI> output = new HashMap<>();
        
        /**
         * Map node names to execution listeners. A special key of {@link Workflow#DEFAULT_LISTENER_KEY}
         * is reserved for binding listeners to events not targeting a specific node (but the
         * entire workflow).  
         */
        private Map<String, List<WorkflowExecutionListener>> listeners = new HashMap<>();
        
        Builder(UUID id, Path dataDir)
        {
            Assert.notNull(id, "Expected a non-null identifier");
            Assert.notNull(dataDir, "Expected a non-null directory path");
            this.id = id;
            this.dataDir = dataDir;
        }
        
        public Builder job(JobDefinition definition)
        {
            Assert.notNull(definition, "Expected a non-empty job definition");
            Assert.isTrue(defs.stream().noneMatch(y -> y.name().equals(definition.name())),
                "A job node with same name is already defined!");
            this.defs.add(definition);
            return this;
        }
        
        public Builder job(Consumer<JobDefinitionBuilder> configurer)
        {
            Assert.notNull(configurer, "Expected a configurer for a job");
            JobDefinitionBuilder definitionBuilder = new JobDefinitionBuilder();
            configurer.accept(definitionBuilder);
            return job(definitionBuilder.build());
        }

        /**
         * Designate an output of a job node as (part of) output of the entire workflow. 
         * 
         * @param nodeName The name of the job node that produces this output.
         * @param path The output path relative to node's staging output directory
         * @param fileName The destination file to copy this output to. If <tt>null</tt>,
         *   the file name of the source path (i.e.{@link path}) will be used. 
         */
        public Builder output(String nodeName, Path path, String fileName)
        {
            Assert.isTrue(!StringUtils.isEmpty(nodeName), "Expected a name (nodeName) for a job node");
            Assert.notNull(path, "Expected an output path");
            
            if (fileName == null)
                fileName = path.getFileName().toString();
            
            Assert.isTrue(!StringUtils.isEmpty(fileName), "Expected a non-empty fileName");
            Assert.isTrue(!output.containsKey(fileName), String.format( 
                "An output file named as [%s] is already present", fileName));
            
            URI outputUri = Result.of(nodeName, path).toUri();
            
            output.put(fileName, outputUri);
            
            return this;
        }
        
        public Builder output(String nodeName, Path path)
        {
            return output(nodeName, path, null);
        }
        
        public Builder output(String nodeName, String path)
        {
            return output(nodeName, Paths.get(path), null);
        }
        
        public Builder output(String nodeName, String path, String fileName)
        {
            return output(nodeName, Paths.get(path), fileName);
        }
        
        /**
         * Register an execution listener.
         * 
         * @param listener
         */
        public Builder listener(WorkflowExecutionListener listener)
        {
            Assert.notNull(listener, "Expected a non-null execution listener");
            this.listeners.computeIfAbsent(DEFAULT_LISTENER_KEY, k -> new ArrayList<>())
                .add(listener);
            return this;
        }
        
        /**
         * Register an execution listener targeting a specific job node.
         * 
         * @param nodeName The name of the job node
         * @param listener 
         */
        public Builder listener(String nodeName, WorkflowExecutionListener listener)
        {
            Assert.notNull(listener, "Expected a non-null execution listener");
            Assert.isTrue(!StringUtils.isEmpty(nodeName), "Expected a non-empty node name");
            this.listeners.computeIfAbsent(nodeName, k -> new ArrayList<>())
                .add(listener);
            return this;
        }
        

        /**
         * Register an execution listener targeting a specific job node.
         * 
         * @param nodeName The name of the job node
         * @param listener 
         */
        public Builder listener(String nodeName, JobExecutionListener listener)
        {
            Assert.notNull(listener, "Expected a non-null execution listener");
            // Adapt listener to a sub-interface of WorkflowExecutionListener
            WorkflowExecutionEventListener workflowExecutionListener = new WorkflowExecutionEventListenerSupport()
            {
                @Override
                public void beforeNode(WorkflowExecutionSnapshot workflowExecutionSnapshot,
                    String nodeName, JobExecution jobExecution)
                {
                    listener.beforeJob(jobExecution);
                }
                
                @Override
                public void afterNode(WorkflowExecutionSnapshot workflowExecutionSnapshot,
                    String nodeName, JobExecution jobExecution)
                {
                    listener.afterJob(jobExecution);
                }
            };
            return listener(nodeName, workflowExecutionListener);
        }
        
        /**
         * Build a job definition by expanding glob-style input wildcards (if any).
         * 
         * @param def The job definition to apply to
         */
        private JobDefinition expandDefinition(JobDefinition def)
        {
            // Check if any glob-style inputs are present in this job definition
            
            boolean shouldExpand = def.inputMap().values().stream()
                .flatMap(List::stream)
                .map(Result::parse)
                .filter(Objects::nonNull)
                .anyMatch(res -> pathMatcher.isPattern(res.outputPath().toString()));
            
            if (!shouldExpand)
                return def;
            
            // The definition contains input with glob-style wildcards to be expanded
            
            Map<String, List<URI>> input = def.inputMap().entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> expandInput(e.getValue())));
            
            return def.withInput(Collections.unmodifiableMap(input));
        }
        
        private List<URI> expandInput(List<URI> inputUris)
        {            
            final List<URI> expandedUris = new ArrayList<>();
            
            for (URI uri: inputUris) {
                final Result res = Result.parse(uri);
                final String pattern = res == null? null : res.outputPath().toString();
                if (pattern != null && pathMatcher.isPattern(pattern)) {
                    // Expand to matching outputs from our dependency node
                    JobDefinition dependency = definitionByName(res.nodeName());
                    for (Path path: dependency.output()) {
                        if (pathMatcher.match(pattern, path.toString())) {
                            expandedUris.add(Result.of(res.nodeName(), path).toUri());
                        }
                    }
                } else {
                    expandedUris.add(uri);
                }
            }
            
            return expandedUris;
        }
        
        private JobDefinition definitionByName(String name)
        {
            return this.defs.stream()
                .filter(y -> y.name().equals(name))
                .findFirst()
                .get();
        }
        
        public Workflow build()
        {        
            Assert.state(!defs.isEmpty(), "The workflow contains no job definitions");
            
            final UUID workflowId = this.id;
            final Path workflowDataDir = this.dataDir.resolve(workflowId.toString());
            
            final List<JobDefinition> defs = Collections.unmodifiableList(
                this.defs.stream()
                    .map(def -> expandDefinition(def))
                    .collect(Collectors.toList()));
            
            final Map<String, URI> output = Collections.unmodifiableMap(new HashMap<>(this.output));
            
            Workflow workflow = new Workflow(workflowId, workflowDataDir, defs, output);
            
            // Setup listeners
                        
            
            if (!this.listeners.isEmpty()) {
                final Map<String, List<WorkflowExecutionListener>> listeners = 
                    this.listeners.size() > 3? (new HashMap<>()) : (new Flat3Map<>());
                this.listeners.forEach((key, handlers) -> {
                    listeners.put(key, Collections.unmodifiableList(new ArrayList<>(handlers)));
                });
                workflow.setListeners(Collections.unmodifiableMap(listeners));
            }
            
            return workflow;
        }
    }
    
    private final UUID id;
    
    /**
     * The data directory for this workflow. 
     */
    private final Path dataDir;
    
    /**
     * The definitions for the jobs that comprise this workflow 
     */
    private final List<JobDefinition> defs;
    
    /**
     * An 1-1 map of node names to integers in <tt>[0..N-1]</tt>, where <tt>N</tt> 
     * is the number of nodes.
     */
    private final Map<String, Integer> nameMap;
    
    /**
     * The dependency graph of this workflow.
     */
    private final DependencyGraph dependencyGraph;
    
    /**
     * The map of registered execution listeners.
     * 
     * A workflow-wide listener is keyed to <tt>null</tt>. A node-wide listener is keyed
     * to the corresponding (non-empty) node name.  
     */
    private Map<String, List<WorkflowExecutionListener>> listeners = Collections.emptyMap();
    
    /**
     * A list of intermediate results that are to be considered as final results of
     * the entire workflow.
     */
    private final Map<String,URI> output;

    private Workflow(UUID id, Path dataDir, List<JobDefinition> defs, Map<String,URI> output)
    {
        this.id = id;
        this.dataDir = dataDir;
        this.defs = defs;
        
        final int n = defs.size();
        
        // Map node names to integer vertices
        
        this.nameMap = Collections.unmodifiableMap(
            IntStream.range(0, n).boxed()
                .collect(Collectors.toMap(v -> defs.get(v).name(), Function.identity())));
        
        // Build dependency graph
        
        this.dependencyGraph = DependencyGraphs.create(n);
        for (JobDefinition def: defs) {
            final int v = this.nameMap.get(def.name()); 
            for (URI inputUri: def.input()) {
                if (inputUri.getScheme().equals(Result.SCHEME)) {
                    Result res = Result.of(inputUri);
                    String dependencyName = res.nodeName();
                    int u = this.nameMap.getOrDefault(dependencyName, -1);
                    Assert.state(u >= 0, 
                        String.format("The job node [%s] depends on an undefined node [%s]", 
                            def.name(), dependencyName));
                    Assert.state(res.outputPath.equals(Result.DOT_PATH) || 
                            defs.get(u).output().contains(res.outputPath()),
                        String.format("The job node [%s] depends on an unknown result: %s", 
                            def.name(), res));
                    this.dependencyGraph.addDependency(v, u);
                }
            }
        }
        
        // Check if dependency graph is a DAG
        
        try {
            DependencyGraphs.check(this.dependencyGraph);
        } catch (CycleDetected cycle) {
            Assert.state(false, String.format(
                "The workflow has cyclic dependencies on job node [%s]", 
                defs.get(cycle.root()).name()));
        }
        
        // Assign what is considered as the output of the workflow.
        // Check that each output URI is a designated output from referenced job node
        
        for (URI outputUri: output.values()) {
            final Result res = Result.of(outputUri);
            final int v = nameMap.getOrDefault(res.nodeName(), -1); 
            Assert.isTrue(v >= 0, 
                String.format("The node [%s] does not exist", res.nodeName()));
            Assert.state(defs.get(v).output().contains(res.outputPath()),
                String.format("The node [%s] does not output at %s", res.nodeName(), res.outputPath()));
        }
        
        this.output = output;
        
        // Check that every context key is exported by some other node
        
        for (JobDefinition def: defs) {
            for (URI entryUri: def.contextMap().values()) {
                final Result res = Result.of(entryUri);
                final String entryKey = res.key();
                int u = this.nameMap.getOrDefault(res.nodeName(), -1);
                Assert.state(u >= 0, 
                    String.format("The job node [%s] imports context from an undefined node [%s]", 
                        def.name(), res.nodeName()));
                Assert.state(defs.get(u).exportedKeys().contains(entryKey),
                    String.format("The job node [%s] does not export an entry named [%s]", 
                        defs.get(u).name(), entryKey));
            }
        }
    } 
    
    private void setListeners(Map<String, List<WorkflowExecutionListener>> listeners)
    {
        Assert.notEmpty(listeners, "Expected a non-empty map of listeners");
        Assert.isTrue(listeners.keySet().stream()
                .allMatch(k -> k.equals(DEFAULT_LISTENER_KEY) || nameMap.containsKey(k)), 
            "Expected all keys to be either null or an existing node name");
        Assert.isTrue(listeners.values().stream().allMatch(x -> !x.isEmpty()), 
            "Expected a non-empty list of listeners");
        this.listeners = listeners;
    }
    
    /**
     * Map a URI to an absolute file path.
     * 
     * <p>This method may return <tt>null</tt> in case a uri is a result (res://) URI
     * with an empty path (which is legal for this kind of URIs). 
     * 
     * @param uri A uri refering either to a file ({@code file://}) or to a result ({@code res://})
     * @return an absolute file path if the <tt>uri</tt> can be mapped, or else <tt>null</tt>.
     * 
     * @see Result
     */
    private Path convertUriToAbsolutePath(URI uri)
    {
        Path path = null;
        String scheme = uri.getScheme();
        if (scheme.equals("file")) {
            path = Paths.get(uri);
        } else if (scheme.equals(Result.SCHEME)) {
            Result result = Result.of(uri);
            if (!result.outputPath().equals(Result.DOT_PATH))
                path = dataDir.resolve(result.path());
        } else {
            Assert.state(false, "Encountered an unknown URI scheme [" + scheme + "]");
        }
        return path;
    }
    
    protected Iterable<Integer> vertices()
    {
        final int n = defs.size();
        return () -> IntStream.range(0, n).boxed().iterator();
    }
    
    public UUID id()
    {
        return id;
    }
    
    /**
     * The data directory for this workflow.
     */
    public Path dataDirectory()
    {
        return dataDir;
    }
    
    /**
     * The staging directory for a given job node.
     *  
     * @param nodeName The name of the job node
     */
    public Path stagingDirectory(String nodeName)
    {
        int vertex = nameMap.getOrDefault(nodeName, -1);
        Assert.isTrue(vertex >= 0, "No job node is named as [" + nodeName + "]");
        Path dir = Paths.get("stage", nodeName);
        return dataDir.resolve(dir);
    }
    
    /**
     * The output directory for this workflow. This will always be under data directory
     * (i.e {@link Workflow#dataDirectory()}).
     * 
     * <p>The output directory is the destination for results designated as output of the entire
     * workflow (and not just as a node's output result). 
     */
    public Path outputDirectory()
    {
        return dataDir.resolve(OUTPUT_DIR);
    }
    
    /**
     * List names of job nodes of this workflow
     */
    public Set<String> nodeNames()
    {
        return nameMap.keySet();
    }
    
    /**
     * The number of job nodes in this workflow 
     */
    public int size()
    {
        return defs.size();
    }
    
    /**
     * Map (expected) output files to absolute filesystem paths
     */
    public Map<String,Path> output()
    {
        return Collections.unmodifiableMap(
            output.entrySet().stream()
                .collect(Collectors.toMap(
                    e -> e.getKey(),
                    e -> dataDir.resolve(Result.of(e.getValue()).path()))));
    }
    
    /**
     * Map (expected) output file to absolute filesystem paths
     * 
     * @param fileName The name of the output (as given in the workflow builder)
     * @return a path, or <tt>null</tt> if no output exists with such a name  
     */
    public Path output(String fileName)
    {
        URI resultUri = output.get(fileName);
        if (resultUri != null)
            return dataDir.resolve(Result.of(resultUri).path());
        return null;
    }
    
    /**
     * Get the names for (expected) output files
     * @return
     */
    public Set<String> outputNames()
    {
        return Collections.unmodifiableSet(output.keySet());
    }
    
    /**
     * Map (expected) output files to result URIs
     */
    public Map<String,URI> outputUris()
    {
        return output;
    }
    
    /**
     * Get the job node by its name
     * 
     * @param nodeName The name of this job node as it was assigned in the original 
     * {@link JobDefinition}.
     */
    public JobNode node(String nodeName)
    {
        int vertex = nameMap.getOrDefault(nodeName, -1);
        Assert.isTrue(vertex >= 0, "No job node is named as [" + nodeName + "]");
        return new JobNode(vertex);
    }
    
    /**
     * Get the job node by its vertex number. 
     * 
     * <p>Note: This method is designed for internal use from cooperating classes of 
     * same package. 
     * 
     * @param vertex The vertex number
     */
    protected JobNode node(int vertex)
    {
        Assert.state(vertex >= 0 && vertex < defs.size(), "The vertex number is invalid");
        return new JobNode(vertex);
    }
    
    /**
     * Iterate on the set of job nodes. 
     * 
     * <p>The iteration order is same with the order of definition of each job (when 
     * the workflow was built). 
     */
    public Iterable<JobNode> nodes()
    {
        final Workflow workflow = this;
        final int n = defs.size();
        return () -> IntStream.range(0, n)
            .mapToObj(v -> workflow.new JobNode(v))
            .iterator();
    }
    
    /**
     * Iterate on the set of job nodes
     * 
     * <p>The iteration order follows a topological ordering of nodes (this is not unique).
     */
    public Iterable<JobNode> nodesInTopologicalOrder()
    {
        final Workflow workflow = this;
        Iterable<Integer> vertices = null; 
        try {
            vertices = DependencyGraphs.topologicalSort(dependencyGraph);
        } catch (CycleDetected e) {
            Assert.state(false, "Did not expect a cycle of dependencies");
        }
        return IterableUtils.transformedIterable(vertices, v -> workflow.new JobNode(v));
    }
    
    /**
     * Get listeners bound to the entire workflow
     */
    public List<WorkflowExecutionListener> getListeners()
    {
        return listeners.containsKey(DEFAULT_LISTENER_KEY)? 
            listeners.get(DEFAULT_LISTENER_KEY) : Collections.emptyList();
    }
    
    /**
     * Get listeners bound to a specific job node
     * 
     * @param nodeName The name of the job node
     */
    public List<WorkflowExecutionListener> getListeners(String nodeName)
    {
        return listeners.containsKey(nodeName)? 
            listeners.get(nodeName) : Collections.emptyList();
    }
    
    @Override
    public String toString()
    {
        return String.format("Workflow [id=%s, dataDir=%s, names=%s]", 
            id, dataDir, nodeNames());
    }

    public String debugGraph()
    {
        return DependencyGraphs.toString(
            dependencyGraph, 
            u -> defs.get(u).name(), 
            ExportDependencyGraph.Direction.LR);
    }
    
    protected String debugGraph(IntFunction<ExportDependencyGraph.NodeAttributes> styleMapper)
    {
        return DependencyGraphs.toString(
            dependencyGraph, 
            u -> defs.get(u).name(), 
            styleMapper,
            ExportDependencyGraph.Direction.LR);
    }
}
