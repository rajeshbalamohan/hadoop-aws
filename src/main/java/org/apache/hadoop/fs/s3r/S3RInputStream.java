/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3r;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;

/**
 * With fixes for S3A
 */
public class S3RInputStream extends FSInputStream {

  public static final Logger LOG = S3RFileSystem.LOG;
  public static final long CLOSE_THRESHOLD = 8 * 1024;

  private FileSystem.Statistics stats;

  private AmazonS3Client client;
  private String bucket;
  private String key;
  private long contentLength;
  private S3ObjectInputStream s3InputStream;
  private boolean closed;

  private long pos;

  //useful for lazy seek
  private long nextReadPos;

  //Amount of data requested from the request
  private long requestedStreamLen;

  public S3RInputStream(String bucket, String key, long contentLength, AmazonS3Client client,
      FileSystem.Statistics stats) {
    LOG.info("Using " + this.getClass().getName());
    this.bucket = bucket;
    this.key = key;
    this.contentLength = contentLength;
    this.client = client;
    this.stats = stats;
    this.pos = 0;
    this.nextReadPos = 0;
    this.closed = false;
    this.s3InputStream = null;
  }

  /**
   * Opens up the stream at specified target position and for given length.
   *
   * @param targetPos
   * @param length
   * @throws IOException
   */
  private synchronized void reopen(long targetPos, long length)
      throws IOException {

    requestedStreamLen = (length < 0) ? this.contentLength
        : Math.max(CLOSE_THRESHOLD, Math.max(CLOSE_THRESHOLD, (targetPos + length)));

    if (s3InputStream != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Closing the previous stream");
      }
      closeStream(requestedStreamLen);
    }

    validateTargetPosition(targetPos);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Requesting for "
              + "targetPos=" + targetPos
              + ", length=" + length
              + ", requestedStreamLen=" + requestedStreamLen
              + ", streamPosition=" + pos
              + ", nextReadPosition=" + nextReadPos
      );
    }

    GetObjectRequest request = new GetObjectRequest(bucket, key)
        .withRange(targetPos, requestedStreamLen);
    s3InputStream = client.getObject(request).getObjectContent();

    if (s3InputStream == null) {
      throw new IOException("Null IO stream");
    }

    this.pos = targetPos;
  }

  private void validateTargetPosition(long targetPos) throws EOFException {

    if (targetPos < 0) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK
          + " " + targetPos);
    }

    if (contentLength > 0 && targetPos > contentLength - 1) {
      throw new EOFException(
          FSExceptionMessages.CANNOT_SEEK_PAST_EOF + " " + targetPos);
    }
  }

  @Override
  public synchronized long getPos() throws IOException {
    return pos;
  }

  @Override
  public void seek(long targetPos) throws IOException {
    checkNotClosed();
    if (targetPos < 0) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK + " " + targetPos);
    }
    // Lazy seek
    nextReadPos = targetPos;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Setting nextReadPos=" + nextReadPos + ", streamPos=" + pos);
    }
  }

  /**
   * Position the stream to a specific position.
   *
   * @param targetPos
   * @param length    length of content that needs to be read from targetPos
   * @throws IOException
   */
  private void seekInStream(long targetPos, long length) throws IOException {
    checkNotClosed();

    if (s3InputStream == null) {
      return;
    }

    if (targetPos == pos) {
      // already at specified position
      if (pos + length <= requestedStreamLen) {
        //still have enough room in the stream
        return;
      }
    }

    // compute how much more to skip
    long diff = targetPos - pos;
    if (targetPos > pos) {
      if ((diff + length) <= s3InputStream.available()) {
        // already available in buffer
        pos += s3InputStream.skip(diff);
        if (pos != targetPos) {
          throw new IOException("Failed to seek to " + targetPos
              + ". Current position " + pos);
        }
        return;
      }
    }

    // close the stream and open at desired position
    pos = targetPos;
    closeStream(this.requestedStreamLen);
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public synchronized int read() throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("read(); streamPos=" + pos + ", nextReadPos=" + nextReadPos);
    }

    seekInStream(nextReadPos, 1);

    if (s3InputStream == null) {
      reopen(nextReadPos, -1);
    }

    int byteRead;
    try {
      byteRead = s3InputStream.read();
    } catch (SocketTimeoutException e) {
      LOG.info("Got timeout while trying to read from stream, trying to recover " + e);
      reopen(pos, -1);
      byteRead = s3InputStream.read();
    } catch (SocketException e) {
      LOG.info("Got socket exception while trying to read from stream, trying to recover " + e);
      reopen(pos, -1);
      byteRead = s3InputStream.read();
    }

    if (byteRead >= 0) {
      pos += byteRead;
      nextReadPos += byteRead;
    }

    if (stats != null && byteRead >= 0) {
      stats.incrementBytesRead(1);
    }
    return byteRead;
  }

  @Override
  public synchronized int read(byte[] buf, int off, int len) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("read(buf,off,len); streamPos=" + pos + ", nextReadPos=" + nextReadPos + ", "
          + "len=" + len + ", off=" + off);
    }
    seekInStream(nextReadPos, len);

    if (s3InputStream == null) {
      reopen(nextReadPos, len);
    }

    int byteRead;
    try {
      byteRead = s3InputStream.read(buf, off, len);
      if (byteRead > 0) {
        pos += byteRead;
        nextReadPos += byteRead;
      }
    } catch (SocketTimeoutException e) {
      LOG.info("Got timeout while trying to read from stream, trying to recover " + e);
      reopen(pos, len);
      byteRead = s3InputStream.read(buf, off, len);
    } catch (SocketException e) {
      LOG.info("Got socket exception while trying to read from stream, trying to recover " + e);
      reopen(pos, len);
      byteRead = s3InputStream.read(buf, off, len);
    }

    if (stats != null && byteRead > 0) {
      stats.incrementBytesRead(byteRead);
    }

    return byteRead;
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Opening for readFully at " + position + " for len=" + length + ", pos=" + pos);
    }

    reopen(position, length);
    int nread = 0;
    while (nread < length) {
      int nbytes = s3InputStream.read(buffer, offset + nread, length - nread);

      pos += nbytes;
      nextReadPos += nbytes;

      if (nbytes < 0) {
        throw new EOFException("End of file reached before reading fully.");
      }
      nread += nbytes;
    }

    if (stats != null) {
      stats.incrementBytesRead(length);
    }
  }

  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    super.close();
    closed = true;
    closeStream(this.contentLength);
  }

  private void closeStream(long contentLength) throws IOException {
    if (s3InputStream != null) {
      if (contentLength - pos <= CLOSE_THRESHOLD) {
        s3InputStream.close();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Closed stream; streamPos=" + pos + ", nextReadPos=" + nextReadPos + ", "
              + "contentLength=" + contentLength);
        }
      } else {
        // Abort, rather than just close, the underlying stream.  Otherwise, the
        // remaining object payload is read from S3 while closing the stream.
        s3InputStream.abort();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Closed stream with abort(); streamPos=" + pos + ", nextReadPos=" +
              nextReadPos + ", " + "contentLength=" + contentLength);
        }
      }
      s3InputStream = null;
    }
  }

  @Override
  public synchronized int available() throws IOException {
    checkNotClosed();

    long remaining = this.contentLength - this.pos;
    if (remaining > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int) remaining;
  }

  @Override
  public boolean markSupported() {
    return false;
  }
}