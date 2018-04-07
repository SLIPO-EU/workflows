package eu.slipo.workflows.util.tasklet;

import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

public class ConcatenateFilesTasklet implements Tasklet
{
    private final List<Path> input;

    private final Path outputDir;

    private final String outputName;
    
    private final byte[] separator;

    public ConcatenateFilesTasklet(List<Path> input, Path outputDir, String outputName, byte[] separator)
    {
        Assert.notNull(outputDir, "An output directory is required");
        Assert.isTrue(outputDir.isAbsolute(), "The output directory is expected as an absolute path");
        Assert.notNull(outputName, "An output name is required");
        Assert.notEmpty(input, "A non-empty input list is required");
        this.input = input;
        this.outputDir = outputDir;
        this.outputName = outputName;
        this.separator = separator;
    }
    
    public ConcatenateFilesTasklet(List<Path> input, Path outputDir, String outputName)
    {
        this(input, outputDir, outputName, null);
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
        throws Exception
    {
        StepExecution stepExecution = chunkContext.getStepContext().getStepExecution();
        ExecutionContext executionContext = stepExecution.getExecutionContext();

        // Create parent directory if needed

        try {
            Files.createDirectories(outputDir);
        } catch (FileAlreadyExistsException ex) {
            // no-op
        }

        Assert.state(Files.isDirectory(outputDir) && Files.isWritable(outputDir),
            "Expected outputDir to be a writable directory");

        // Concatenate input into target

        Path output = outputDir.resolve(outputName);

        OpenOption[] outputOptions = new StandardOpenOption[] { 
            StandardOpenOption.WRITE, StandardOpenOption.CREATE 
        };
              
        try (OutputStream out = Files.newOutputStream(output, outputOptions)) {
            boolean first = true;
            for (Path inputPath: input) {
                if (!first && separator != null) 
                    out.write(separator);
                Files.copy(inputPath, out);
                first = false;
            }
        }

        // Update execution context

        executionContext.put("outputDir", outputDir.toString());
        executionContext.put("outputName", outputName);

        return RepeatStatus.FINISHED;
    }
}
