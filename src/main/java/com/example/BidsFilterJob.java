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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * A word count example for Apache Crunch, based on Crunch's example projects.
 */
public class BidsFilterJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        validateArgs(args);
        List<String> paths = new ArrayList<String>();
        BufferedReader bufferedReader = null;
        try {
            bufferedReader = new BufferedReader(new FileReader(args[0]));
            String line;
            line = bufferedReader.readLine();
            paths.addAll(Arrays.asList(line.split(" ")));
        } finally {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
        }
        paths.add(args[1]);

        String[] strings = Arrays.copyOf(paths.toArray(), paths.size(), String[].class);

        ToolRunner.run(new Configuration(), new BidsFilterJob(), strings);
    }

    public int run(String[] args) throws Exception {

        List<String> paths = new ArrayList<String>(Arrays.asList(args));

        if (paths.size() < 2) {
            throw new Exception("Empty input");
        }

        String outputPath = paths.remove(paths.size() - 1);

        Configuration c = getConf();

        String firstInput = paths.remove(0);
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