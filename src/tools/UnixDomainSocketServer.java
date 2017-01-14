/**
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
 */
package net.opentsdb.tools;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import net.opentsdb.core.TSDB;
import net.opentsdb.tsd.PipelineFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Title: UnixDomainSocketServer</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.tools.UnixDomainSocketServer</code></p>
 * Trace like this:  echo "put foo.bar 1460070776 5 dc=dc5 host=host5" | netcat -U /tmp/tsdb.sock
 */

public class UnixDomainSocketServer  {
	final  EventLoopGroup bossGroup = new EpollEventLoopGroup(1);
	final  EventLoopGroup workerGroup = new EpollEventLoopGroup(1);
	final  ServerBootstrap b = new ServerBootstrap();
	final PipelineFactory pipelineFactory;
	final Channel serverChannel;
	final DomainSocketAddress socketAddress;
	final Logger log = LoggerFactory.getLogger(getClass());
	/**
	 * Creates a new UnixDomainSocketServer
	 */
	public UnixDomainSocketServer(final String path, final TSDB tsdb) {
		pipelineFactory = new PipelineFactory(tsdb);
		b.group(bossGroup, workerGroup)
			.channel(EpollServerDomainSocketChannel.class)
			.handler(new LoggingHandler(LogLevel.INFO))
			.childHandler(pipelineFactory);
		socketAddress = new DomainSocketAddress(path);
		try {
			serverChannel = b.bind(socketAddress).sync().channel();
			log.error("Started UnixDomainServer on [{}]", path);
		} catch (Exception ex) {
			log.error("Failed to start UnixDomainServer on [{}]", path, ex);
			throw new RuntimeException(ex);
		}
		  
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
