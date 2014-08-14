/*
 * Copyright (c) 2008 Bradley W. Kimmel
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package ca.eandb.jdcp.job;

import java.io.FileOutputStream;
import java.io.RandomAccessFile;

/**
 * Provides access to host services on the server for a
 * <code>ParallelizableJob</code> running in a secure sandbox.
 * @author Brad Kimmel
 * @see ca.eandb.jdcp.job.ParallelizableJob#initialize(Host)
 */
public interface HostService {

  /**
   * Creates a file in the job's working directory.
   * @param path The path of the file to create, relative to the job's
   *     working directory.
   * @return A <code>RandomAccessFile</code> for the newly created file.
   * @throws IllegalArgumentException If the path is absolute or refers
   *     to the parent directory ("..").
   */
  RandomAccessFile createRandomAccessFile(String path);

  /**
   * Creates a file in the job's working directory.
   * @param path The path of the file to create, relative to the job's
   *     working directory.
   * @return A <code>FileOutputStream</code> for the newly created file.
   * @throws IllegalArgumentException If the path is absolute or refers
   *     to the parent directory ("..").
   */
  FileOutputStream createFileOutputStream(String path);

}
