package de.dimajix.training.hadoop.skeleton;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class Driver extends Configured implements Tool {
    private static Logger log = LoggerFactory.getLogger(Driver.class);

    @Option(name = "-i", aliases = "--inputDir", usage = "input directory", required = true)
    private String inputDir;

    @Option(name = "-o", aliases = "--outputDir", usage = "output directory", required = true)
    private String outputDir;

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Driver(), args);
        System.exit(ret);
    }

    @Override
    public int run(String[] args) {
        parseCmdlOptions(args);

        // set up basic job information
        Configuration conf = getConf();

        try {
            runJob(conf);
        }
        catch(IOException|InterruptedException|ClassNotFoundException|URISyntaxException ex) {
            throw new RuntimeException(ex.getMessage());
        }

        return 0;
    }

    private boolean runJob(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Job job = Job.getInstance(conf, "Hadoop Job Skeleton");
        job.setJarByClass(Driver.class);

        // Configure input format and files
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(inputDir));

        // Configure output format and files
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        // set up mapper, combiner and reducer
        job.setMapperClass(SkeletonMapper.class);
        job.setReducerClass(SkeletonReducer.class);

        // set sorting, grouping and partitioning
        // set key and value types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        return job.waitForCompletion(true);
    }

    private void parseCmdlOptions(String... args) {
        CmdLineParser parser = new CmdLineParser(this);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            // if there's a problem in the command line, you'll get this exception. this will report an error message.
            String jar = ClassUtil.findContainingJar(this.getClass());
            System.err.println(e.getMessage());
            System.err.println("hadoop jar " + jar + " " + this.getClass().getName() + " [options...] arguments...");
            parser.printUsage(System.err);
            System.err.println();
            System.exit(1);
        }
    }
}
