package com.example;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.List;


/**
 * A word count example for Apache Crunch, based on Crunch's example projects.
 */
public class BidsFilterJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        validateArgs(args);
        ToolRunner.run(new Configuration(), new BidsFilterJob(), args);
    }

    public int run(String[] args) throws Exception {
        List<String> paths;
        try(BufferedReader bufferedReader = new BufferedReader(new FileReader(args[0]))) {
            String line;
            line = bufferedReader.readLine();
            paths = Arrays.asList(line.split(" "));
        }


        if (paths.size() == 0) {
            throw new Exception("Empty input");
        }

        String firstInput = paths.get(0);
        String outputPath = args[1];

        Configuration c = getConf();

        Pipeline pipeline = new MRPipeline(BidsFilterJob.class, c);

        PCollection<String> all_lines = pipeline.read(From.textFile(firstInput));

        for (String path : paths) {
            PCollection<String> lines = pipeline.read(From.textFile(path));
            all_lines = all_lines.union(lines);
        }

        PCollection<String> vids = all_lines.parallelDo(new BidsFilter(), Writables.strings());

        pipeline.writeTextFile(vids, outputPath);

        PipelineResult result = pipeline.done();

        return result.succeeded() ? 0 : 1;
    }

    private static boolean validateArgs(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: hadoop jar crunch-demo-1.0-SNAPSHOT-job.jar"
                    + " [generic options] input output");
            System.err.println();
            GenericOptionsParser.printGenericCommandUsage(System.err);
            return true;
        }
        return false;
    }
}
