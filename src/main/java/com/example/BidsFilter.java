package com.example;
import org.apache.crunch.Emitter;
import java.util.Arrays;
import java.util.List;

public class BidsFilter extends ConfiguredDoFn {

  @Override
  public void process(String line, Emitter<String> emitter) {

    String[] words = line.split("\\t+");
    List<Integer> campaigns = Arrays.asList(818, 819);
    long start = 1435536000000L;
    long finish = 1436140740000L;

    int cid = 0;
    long ts = 0;

    for (int i = 0; i < words.length; i++) {
      if (words[i].equals("cid")) {
        cid = Integer.valueOf(words[i + 1]);
      }
      if (words[i].equals("_ts")) {
        ts = Long.valueOf(words[i + 1]);
      }
    }

    if ((ts > start) && (ts < finish) && (campaigns.contains(cid))) {
      emitter.emit(line);
    }
  }
}
