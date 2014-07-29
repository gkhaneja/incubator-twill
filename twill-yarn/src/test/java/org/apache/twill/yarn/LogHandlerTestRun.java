/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.yarn;

import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.api.logging.LogThrowable;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Test for LogHandler able to receive logs from AM and runnable.
 */
public class LogHandlerTestRun extends BaseYarnTest {

  @Test
  public void testLogHandler() throws ExecutionException, InterruptedException {
    final CountDownLatch latch = new CountDownLatch(3);
    final Queue<LogThrowable> throwables = new ConcurrentLinkedQueue<LogThrowable>();

    LogHandler logHandler = new LogHandler() {
      @Override
      public void onLog(LogEntry logEntry) {
        // Would expect logs from AM and the runnable.
        if (logEntry.getMessage().startsWith("Starting runnable " + LogRunnable.class.getSimpleName())) {
          latch.countDown();
        } else if (logEntry.getMessage().equals("Running")) {
          latch.countDown();
        } else if (logEntry.getMessage().equals("Got exception") && logEntry.getThrowable() != null) {
          throwables.add(logEntry.getThrowable());
          latch.countDown();
        }
      }
    };

    TwillRunner runner = YarnTestUtils.getTwillRunner();
    TwillController controller = runner.prepare(new LogRunnable())
                                       .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
                                       .addLogHandler(logHandler)
                                       .start();

    try {
      Assert.assertTrue(latch.await(100, TimeUnit.SECONDS));
    } finally {
      controller.stopAndWait();
    }

    // Verify the log throwable
    Assert.assertEquals(1, throwables.size());

    LogThrowable t = throwables.poll();
    Assert.assertEquals(RuntimeException.class.getName(), t.getClassName());
    Assert.assertNotNull(t.getCause());
    Assert.assertEquals(4, t.getStackTraces().length);

    t = t.getCause();
    Assert.assertEquals(Exception.class.getName(), t.getClassName());
    Assert.assertEquals("Exception", t.getMessage());
  }

  /**
   * TwillRunnable for the test case to simply emit one log line.
   */
  public static final class LogRunnable extends AbstractTwillRunnable {

    private static final Logger LOG = LoggerFactory.getLogger(LogRunnable.class);
    private final CountDownLatch stopLatch = new CountDownLatch(1);

    @Override
    public void run() {
      LOG.info("Running");
      try {
        // Just throw some exception and log it
        try {
          throw new Exception("Exception");
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      } catch (Throwable t) {
        LOG.error("Got exception", t);
      }

      try {
        stopLatch.await();
      } catch (InterruptedException e) {
        LOG.error("Interrupted", e);
      }
    }

    @Override
    public void stop() {
      stopLatch.countDown();
    }
  }
}
