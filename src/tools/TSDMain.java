// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tools;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.tsd.PipelineFactory;
import net.opentsdb.tsd.RpcManager;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.FileSystem;

/**
 * Main class of the TSD, the Time Series Daemon.
 */
final class TSDMain {

  /** Prints usage and exits with the given retval. */
  static void usage(final ArgP argp, final String errmsg, final int retval) {
    System.err.println(errmsg);
    System.err.println("Usage: tsd --port=PORT"
      + " --staticroot=PATH --cachedir=PATH\n"
      + "Starts the TSD, the Time Series Daemon");
    if (argp != null) {
      System.err.print(argp.usage());
    }
    System.exit(retval);
  }

  private static final short DEFAULT_FLUSH_INTERVAL = 1000;
  
  private static TSDB tsdb = null;
  
  private static final Map<String, TSDTCPServer> servers = new ConcurrentHashMap<String, TSDTCPServer>();
  
  public static void main(String[] argsx) throws IOException {
	String[] args = {
			"--port=4242",  
			"--auto-metric",
			"--staticroot=./target/opentsdb-2.2.0/queryui", ///home/nwhitehead/services/opentsdb/build/staticroot", //./build/staticroot/", //)/home/nwhitehead/services/opentsdb/opentsdb/build/staticroot",  
			"--cachedir=/tmp/opentsdb",
			"--config=./src/opentsdb.conf"
	};
		
    Logger log = LoggerFactory.getLogger(TSDMain.class);
    log.info("Starting.");
    log.info(BuildData.revisionString());
    log.info(BuildData.buildString());
    try {
      System.in.close();  // Release a FD we don't need.
    } catch (Exception e) {
      log.warn("Failed to close stdin", e);
    }

    final ArgP argp = new ArgP();
    CliOptions.addCommon(argp);
    argp.addOption("--port", "NUM", "TCP port to listen on.");
    argp.addOption("--bind", "ADDR", "Address to bind to (default: 0.0.0.0).");
    argp.addOption("--staticroot", "PATH",
                   "Web root from which to serve static files (/s URLs).");
    argp.addOption("--cachedir", "PATH",
                   "Directory under which to cache result of requests.");
    argp.addOption("--worker-threads", "NUM",
                   "Number for async io workers (default: cpu * 2).");
    argp.addOption("--async-io", "true|false",
                   "Use async NIO (default true) or traditional blocking io");
    argp.addOption("--backlog", "NUM",
                   "Size of connection attempt queue (default: 3072 or kernel"
                   + " somaxconn.");
    argp.addOption("--flush-interval", "MSEC",
                   "Maximum time for which a new data point can be buffered"
                   + " (default: " + DEFAULT_FLUSH_INTERVAL + ").");
    CliOptions.addAutoMetricFlag(argp);
    args = CliOptions.parse(argp, args);
    args = null; // free().

    // get a config object
    Config config = CliOptions.getConfig(argp);
    
    // check for the required parameters
    try {
      if (config.getString("tsd.http.staticroot").isEmpty())
        usage(argp, "Missing static root directory", 1);
    } catch(NullPointerException npe) {
      usage(argp, "Missing static root directory", 1);
    }
    try {
      if (config.getString("tsd.http.cachedir").isEmpty())
        usage(argp, "Missing cache directory", 1);
    } catch(NullPointerException npe) {
      usage(argp, "Missing cache directory", 1);
    }
    try {
      if (!config.hasProperty("tsd.network.port"))
        usage(argp, "Missing network port", 1);
      config.getInt("tsd.network.port");
    } catch (NumberFormatException nfe) {
      usage(argp, "Invalid network port setting", 1);
    }

    // validate the cache and staticroot directories
    try {
      FileSystem.checkDirectory(config.getString("tsd.http.staticroot"), 
          !Const.MUST_BE_WRITEABLE, Const.DONT_CREATE);
      FileSystem.checkDirectory(config.getString("tsd.http.cachedir"),
          Const.MUST_BE_WRITEABLE, Const.CREATE_IF_NEEDED);
    } catch (IllegalArgumentException e) {
      usage(argp, e.getMessage(), 3);
    }

    
    try {
      tsdb = new TSDB(config);
      tsdb.initializePlugins(true);
      if (config.getBoolean("tsd.storage.hbase.prefetch_meta")) {
        tsdb.preFetchHBaseMeta();
      }
      
      // Make sure we don't even start if we can't find our tables.
      tsdb.checkNecessaryTablesExist().joinUninterruptibly();
      
      registerShutdownHook();
      
      
      // This manager is capable of lazy init, but we force an init
      // here to fail fast.
      final RpcManager manager = RpcManager.instance(tsdb);
      final PipelineFactory pipelineFactory = new PipelineFactory(tsdb, manager);
      final TSDTCPServer server = new TSDTCPServer(tsdb, config, pipelineFactory); 
      servers.put(server.serverURI.toString(), server);
      server.start();
    } catch (Throwable e) {
      // FIXME:  stop server instances
      try {
        if (tsdb != null)
          tsdb.shutdown().joinUninterruptibly();
      } catch (Exception e2) {
        log.error("Failed to shutdown HBase client", e2);
      }
      throw new RuntimeException("Initialization failed", e);
    }
    // The server is now running in separate threads, we can exit main.
  }

  private static void registerShutdownHook() {
    final class TSDBShutdown extends Thread {
      public TSDBShutdown() {
        super("TSDBShutdown");
      }
      public void run() {
        try {
          if (RpcManager.isInitialized()) {
            // Check that its actually been initialized.  We don't want to
            // create a new instance only to shutdown!
            RpcManager.instance(tsdb).shutdown().join();
          }
          if (tsdb != null) {
            tsdb.shutdown().join();
          }
        } catch (Exception e) {
          LoggerFactory.getLogger(TSDBShutdown.class)
            .error("Uncaught exception during shutdown", e);
        }
      }
    }
    Runtime.getRuntime().addShutdownHook(new TSDBShutdown());
  }
}
