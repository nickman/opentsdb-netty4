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
package net.opentsdb.tsd;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.ReferenceCountUtil;

import java.nio.charset.Charset;
import java.util.List;

import net.opentsdb.core.Tags;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Title: StringArrayDecoder</p>
 * <p>Description: Returns the split frames as Strings</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>io.netty.handler.codec.StringArrayDecoder</code></p>
 */
//@ChannelHandler.Sharable
public class StringArrayDecoder extends ByteToMessageDecoder {
	/** The UTF8 character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	
	/** Static class logger */
	protected static final Logger log = LoggerFactory.getLogger(StringArrayDecoder.class);
	
	


	/**
	 * Converts the passed list of ByteBufs into an array of strings
	 * {@inheritDoc}
	 * @see io.netty.handler.codec.ByteToMessageDecoder#decode(io.netty.channel.ChannelHandlerContext, io.netty.buffer.ByteBuf, java.util.List)
	 */
	@Override
	protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out) throws Exception {
		out.add(Tags.splitString(in.toString(UTF8), ' '));
		in.clear();
		ReferenceCountUtil.touch(in, "StringArrayDecoder");
	}
	
	
	

}
