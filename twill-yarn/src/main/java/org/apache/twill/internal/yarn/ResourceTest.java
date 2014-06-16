package org.apache.twill.internal.yarn;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.Services;
import org.apache.twill.yarn.ReactorApp;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.Random;
import java.util.concurrent.ExecutionException;

//import org.apache.twill.api.Locality;
//import org.apache.twill.internal.DefaultResourceSpecification;

/**
 * Created by gourav on 5/29/14.
 */
public class ResourceTest {

  private static Logger logger = LoggerFactory.getLogger(ResourceTest.class);

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Arguments format: <host:port of zookeeper server>");
    }
    String zkStr = args[0];

    final TwillRunnerService twillRunner =
      new YarnTwillRunnerService(
        new YarnConfiguration(), zkStr);
    twillRunner.startAndWait();


    final TwillController controller =
      twillRunner.prepare(new ReactorApp())
        .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
        .start();



    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        controller.stopAndWait();
        twillRunner.stopAndWait();
      }
    });

    try {
      Services.getCompletionFuture(controller).get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }





  private static final class ResourceRunnable extends AbstractTwillRunnable {

    public static Logger logger = LoggerFactory.getLogger(ResourceRunnable.class);
    private volatile Thread runThread;

    @Override
    public void run() {
      runThread = Thread.currentThread();
      Random random = new Random();

      logger.info("Testing: Sleeping for sometime");
      //getContext().announce("sleep", Integer.parseInt(getContext().getSpecification().getConfigs().get("port")));

    }

    @Override
    public void stop() {
      //No op
    }
  }
}
