package com.example;


import org.apache.crunch.DoFn;
import org.apache.hadoop.conf.Configuration;


abstract class ConfiguredDoFn extends DoFn<String, String> {

  @Override
  public void configure(Configuration c) {
    c.setInt("mapred.reduce.max.attempts", 20);
    c.setInt("mapred.max.map.failures.percent", 20);
    c.setInt("mapred.map.max.attempts", 20);
    c.setInt("mapred.max.reduce.failures.percent", 20);
    c.setBoolean("mapred.skip.mode.enabled", true);
    c.setInt("mapred.skip.map.max.skip.records", 1);
    c.setInt("mapred.skip.attempts.to.start.skipping", 2);
    c.setBoolean("mapred.lazy.output.format", true);
    c.set("mapred.job.queue.name", "prio");
  }
}