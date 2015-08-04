package com.example;

import org.apache.crunch.Emitter;


public class VidParser extends ConfiguredDoFn {

    @Override
    public void process(String line, Emitter<String> emitter) {
        String[] words = line.split("\\t+");
        for (int i = 0; i < words.length; i++) {
            if (words[i].equals("_vid")) {
                emitter.emit(words[i + 1]);
            }
        }
    }
}
