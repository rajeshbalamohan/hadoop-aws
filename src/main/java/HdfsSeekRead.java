import com.google.common.base.Stopwatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AInputStream;

import java.io.EOFException;
import java.io.IOException;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
public class HdfsSeekRead {

  //Size to read in every read operation in random read
  static final int READ_SIZE = 1 * 1024;

  //How much to skip in random read. Q: What if someone has a TCP read buffer as 32 MB?
  static final int SKIP_SIZE = 4 * 1024 ;

  /**
   * Read sequentially some "X" MB of data
   *
   * @param fdis
   * @param target
   * @throws IOException
   */
  public static void readSequential(FSDataInputStream fdis, int target)
      throws IOException {
    long totalRead = 0;
    Stopwatch stopWatch = new Stopwatch();
    stopWatch.start();

    byte[] buf = new byte[READ_SIZE];
    while (fdis.available() != -1 && totalRead < (target * READ_SIZE)) {
      /**
       * This would NOT go through pread path in DFSInputStream.
       */
      totalRead += fdis.read(buf);
    }
    System.out.println("Available : " + fdis.available());
    stopWatch.stop();
    System.out.println("Time taken for readSequential : "
        + stopWatch.elapsedMillis() + ", totalRead=" + totalRead);
  }

  /**
   * Seek to a position, read READ_SIZE amount of data.
   * Repeat this "X" number of times.
   * <p/>
   * Ensure to keep it within single block limit (so that it can read from same node)
   *
   * @param fdis
   * @param target number of times random read has to be performed
   * @throws IOException
   * @throws InterruptedException
   */
  public static void readRandom(FSDataInputStream fdis, int target)
      throws IOException, InterruptedException {

    long seekPos = 101;

    Stopwatch stopWatch = new Stopwatch();
    stopWatch.start();

    for (int i = 0; i < target; i++) {
      if (i % 10 == 0) {
        seekPos = 101;
      }
      fdis.seek(seekPos);

      byte[] buffer = new byte[READ_SIZE];

      System.out.println("** CALLING READ..");
      fdis.readFully(seekPos, buffer, 0, READ_SIZE);

      int read = 0;

      System.out.println(
          "Read from seekPos:" + seekPos + ", time taken:" + stopWatch.elapsedMillis() + ", READ:"
              + read);
      System.out.println("DATA for i=" + i + ", " + new String(buffer));

      System.out.println();
      System.out.println();

      seekPos += SKIP_SIZE;
    }
    stopWatch.stop();
    System.out.println("Time taken for readRandom : "
        + stopWatch.elapsedMillis());
  }

  /**
   * Read bytes from the given position in the stream to the given buffer.
   * Continues to read until <code>length</code> bytes have been read.
   *
   * @param file     Stream to read from
   * @param position position in the input stream to seek
   * @param buffer   buffer into which data is read
   * @param offset   offset into the buffer in which data is written
   * @param length   the number of bytes to read
   * @throws EOFException If the end of stream is reached while reading.
   *                      If an exception is thrown an undetermined number
   *                      of bytes in the buffer may have been written.
   */
  public static void readFromStream(FSDataInputStream file, long position,
      byte[] buffer, int offset, int length) throws IOException {
    System.out.println("Class : " + file.getWrappedStream().getClass().getName());
    if (file.getWrappedStream() instanceof S3AInputStream) {
      //Ref: HIVE-11945
      file.seek(position);
      int nread = 0;
      while (nread < length) {
        int nbytes = file.read(buffer, offset + nread, length - nread);
        if (nbytes < 0) {
          throw new EOFException("End of file reached before reading fully.");
        }
        nread += nbytes;
      }
    } else {
      file.readFully(position, buffer, 0, READ_SIZE);
    }
  }

  public static void main(String[] args) throws Exception {
    System.out.println("Starting");

    Configuration conf = new Configuration();
    conf.set("fs.s3r.impl", "org.apache.hadoop.fs.s3r.S3RFileSystem");
    Path path = new Path(args[0]);
    System.out.println("Reading from  " + path);

    boolean seq = (args.length == 3 && args[2].trim().equalsIgnoreCase("seq"));
    try (FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream fdis = fs.open(path)) {

      System.out.println("Going to read in seq : " + seq);
      if (seq) {
        readSequential(fdis, Integer.parseInt(args[1]));
      } else {
        readRandom(fdis, Integer.parseInt(args[1]));
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }

    System.out.println("Done!!");
  }
}
