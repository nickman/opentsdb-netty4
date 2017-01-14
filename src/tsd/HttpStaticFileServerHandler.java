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

import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_MODIFIED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelProgressiveFuture;
import io.netty.channel.ChannelProgressiveFutureListener;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedFile;
import io.netty.util.CharsetUtil;
import io.netty.util.internal.SystemPropertyUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.lang.ref.WeakReference;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;
import java.util.regex.Pattern;

import javax.activation.MimetypesFileTypeMap;

/**
 * <p>Title: HttpStaticFileServerHandler</p>
 * <p>Description: Zero copy static file http server. Taken wholesale from the Netty examples</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.tsd.HttpStaticFileServerHandler</code></p>
 */

public class HttpStaticFileServerHandler { //extends SimpleChannelInboundHandler<FullHttpRequest> {

  /** The HTTP header date format */
  public static final String HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";
  /** The time zone */
  public static final String HTTP_DATE_GMT_TIMEZONE = "GMT";
  /** The cache time for served content */
  public static final int HTTP_CACHE_SECONDS = 60;
  
  private static final ThreadLocal<WeakReference<SimpleDateFormat>> threadDateFormatter = new ThreadLocal<WeakReference<SimpleDateFormat>>() {
  	@Override
  	protected WeakReference<SimpleDateFormat> initialValue() {  		
  		return new WeakReference<SimpleDateFormat>(new SimpleDateFormat(HTTP_DATE_FORMAT));
  	}
  };

  /**
   * Acquires the calling thread's simple date formatter 
   * @return the calling thread's simple date formatter
   */
  public static SimpleDateFormat getDateFormatter() {
  	WeakReference<SimpleDateFormat> ref = threadDateFormatter.get();
  	SimpleDateFormat sdf = ref.get();
  	if(sdf==null) {
  		threadDateFormatter.remove();
  		sdf = threadDateFormatter.get().get();  		
  	}
  	return sdf;
  }
  
  
  
  
  
  //@Override
  //public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
  static void sendFile(final ChannelHandlerContext ctx, final FullHttpRequest request, final String filePath, final int max_age) throws Exception {

      final String path = filePath; //sanitizeUri(filePath);
      if (path == null) {
          sendError(ctx, FORBIDDEN);
          return;
      }

      File file = new File(path);
      if (file.isHidden() || !file.exists()) {
          sendError(ctx, NOT_FOUND);
          return;
      }

      if (file.isDirectory()) {
      	sendError(ctx, FORBIDDEN);
      }

      if (!file.isFile()) {
          sendError(ctx, FORBIDDEN);
          return;
      }

      // Cache Validation
      String ifModifiedSince = request.headers().get(HttpHeaderNames.IF_MODIFIED_SINCE);
      if (ifModifiedSince != null && !ifModifiedSince.isEmpty()) {
          SimpleDateFormat dateFormatter = getDateFormatter(); //new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
          Date ifModifiedSinceDate = dateFormatter.parse(ifModifiedSince);

          // Only compare up to the second because the datetime format we send to the client
          // does not have milliseconds
          long ifModifiedSinceDateSeconds = ifModifiedSinceDate.getTime() / 1000;
          long fileLastModifiedSeconds = file.lastModified() / 1000;
          if (ifModifiedSinceDateSeconds == fileLastModifiedSeconds) {
              sendNotModified(ctx);
              return;
          }
      }

      RandomAccessFile raf;
      try {
          raf = new RandomAccessFile(file, "r");
      } catch (FileNotFoundException ignore) {
          sendError(ctx, NOT_FOUND);
          return;
      }
      long fileLength = raf.length();

      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
  	  response.headers().set(HttpHeaderNames.CACHE_CONTROL,
  			  "max-age=" + max_age);
      
      HttpUtil.setContentLength(response, fileLength);
      setContentTypeHeader(response, file);
      setDateAndCacheHeaders(response, file);
      if (HttpUtil.isKeepAlive(request)) {
          response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
      }

      // Write the initial line and the header.
      ctx.write(response);

      // Write the content.
      ChannelFuture sendFileFuture;
      ChannelFuture lastContentFuture;
      if (ctx.pipeline().get(SslHandler.class) == null) {
          sendFileFuture =
                  ctx.write(new DefaultFileRegion(raf.getChannel(), 0, fileLength), ctx.newProgressivePromise());
          // Write the end marker.
          lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
      } else {
          sendFileFuture =
                  ctx.writeAndFlush(new HttpChunkedInput(new ChunkedFile(raf, 0, fileLength, 8192)),
                          ctx.newProgressivePromise());
          // HttpChunkedInput will write the end marker (LastHttpContent) for us.
          lastContentFuture = sendFileFuture;
      }

      sendFileFuture.addListener(new ChannelProgressiveFutureListener() {
          @Override
          public void operationProgressed(ChannelProgressiveFuture future, long progress, long total) {
              if (total < 0) { // total unknown
                  System.err.println(future.channel() + " Transfer progress: " + progress);
              } else {
                  System.err.println(future.channel() + " Transfer progress: " + progress + " / " + total);
              }
          }

          @Override
          public void operationComplete(ChannelProgressiveFuture future) {
              System.err.println(future.channel() + " Transfer complete.");
          }
      });

      // Decide whether to close the connection or not.
      if (!HttpUtil.isKeepAlive(request)) {
          // Close the connection when the whole content is written out.
          lastContentFuture.addListener(ChannelFutureListener.CLOSE);
      }
  }


  private static final Pattern INSECURE_URI = Pattern.compile(".*[<>&\"].*");

  private static String sanitizeUri(String uri) {
      // Decode the path.
      try {
          uri = URLDecoder.decode(uri, "UTF-8");
      } catch (UnsupportedEncodingException e) {
          throw new Error(e);
      }

      if (uri.isEmpty() || uri.charAt(0) != '/') {
          return null;
      }

      // Convert file separators.
      uri = uri.replace('/', File.separatorChar);

      // Simplistic dumb security check.
      // You will have to do something serious in the production environment.
      if (uri.contains(File.separator + '.') ||
          uri.contains('.' + File.separator) ||
          uri.charAt(0) == '.' || uri.charAt(uri.length() - 1) == '.' ||
          INSECURE_URI.matcher(uri).matches()) {
          return null;
      }

      // Convert to absolute path.
      return SystemPropertyUtil.get("user.dir") + File.separator + uri;
  }

  private static final Pattern ALLOWED_FILE_NAME = Pattern.compile("[A-Za-z0-9][-_A-Za-z0-9\\.]*");


  private static void sendRedirect(ChannelHandlerContext ctx, String newUri) {
      FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, FOUND);
      response.headers().set(HttpHeaderNames.LOCATION, newUri);

      // Close the connection as soon as the error message is sent.
      ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
  }

  private static void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
      FullHttpResponse response = new DefaultFullHttpResponse(
              HTTP_1_1, status, Unpooled.copiedBuffer("Failure: " + status + "\r\n", CharsetUtil.UTF_8));
      response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");

      // Close the connection as soon as the error message is sent.
      ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
  }

  /**
   * When file timestamp is the same as what the browser is sending up, send a "304 Not Modified"
   *
   * @param ctx
   *            Context
   */
  private static void sendNotModified(ChannelHandlerContext ctx) {
      FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, NOT_MODIFIED);
      setDateHeader(response);

      // Close the connection as soon as the error message is sent.
      ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
  }

  /**
   * Sets the Date header for the HTTP response
   *
   * @param response
   *            HTTP response
   */
  private static void setDateHeader(FullHttpResponse response) {
      SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
      dateFormatter.setTimeZone(TimeZone.getTimeZone(HTTP_DATE_GMT_TIMEZONE));

      Calendar time = new GregorianCalendar();
      response.headers().set(HttpHeaderNames.DATE, dateFormatter.format(time.getTime()));
  }

  /**
   * Sets the Date and Cache headers for the HTTP Response
   *
   * @param response
   *            HTTP response
   * @param fileToCache
   *            file to extract content type
   */
  private static void setDateAndCacheHeaders(HttpResponse response, File fileToCache) {
      SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
      dateFormatter.setTimeZone(TimeZone.getTimeZone(HTTP_DATE_GMT_TIMEZONE));

      // Date header
      Calendar time = new GregorianCalendar();
      response.headers().set(HttpHeaderNames.DATE, dateFormatter.format(time.getTime()));

      // Add cache headers
      time.add(Calendar.SECOND, HTTP_CACHE_SECONDS);
      response.headers().set(HttpHeaderNames.EXPIRES, dateFormatter.format(time.getTime()));
      response.headers().set(HttpHeaderNames.CACHE_CONTROL, "private, max-age=" + HTTP_CACHE_SECONDS);
      response.headers().set(
              HttpHeaderNames.LAST_MODIFIED, dateFormatter.format(new Date(fileToCache.lastModified())));
  }

  /**
   * Sets the content type header for the HTTP Response
   *
   * @param response
   *            HTTP response
   * @param file
   *            file to extract content type
   */
  private static void setContentTypeHeader(HttpResponse response, File file) {
      MimetypesFileTypeMap mimeTypesMap = new MimetypesFileTypeMap();
      response.headers().set(HttpHeaderNames.CONTENT_TYPE, mimeTypesMap.getContentType(file.getPath()));
  }
}

