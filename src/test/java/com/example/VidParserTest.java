package com.example;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.apache.crunch.Emitter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class VidParserTest {
  @Mock
  private Emitter<String> emitter;

  @Test
  public void testProcess() {
    VidParser splitter = new VidParser();
    splitter.process("_vid\ttest", emitter);

    verify(emitter).emit("test");
    verifyNoMoreInteractions(emitter);
  }

}
