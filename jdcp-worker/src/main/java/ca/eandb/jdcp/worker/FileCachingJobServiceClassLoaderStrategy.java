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

package ca.eandb.jdcp.worker;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.log4j.Logger;

import ca.eandb.jdcp.remote.JobService;
import ca.eandb.jdcp.remote.TaskService;
import ca.eandb.util.StringUtil;
import ca.eandb.util.io.FileUtil;

/**
 * A <code>CachingJobServiceClassLoaderStrategy</code> that stores class
 * definitions on the file system.
 * @author Brad Kimmel
 */
public final class FileCachingJobServiceClassLoaderStrategy extends
    CachingJobServiceClassLoaderStrategy {

  /** The <code>Logger</code> for this class. */
  private static final Logger logger = Logger.getLogger(FileCachingJobServiceClassLoaderStrategy.class);

  /** The root directory in which to store cached class definitions. */
  private final File directory;

  /**
   * Creates a new <code>FileCachingJobServiceClassLoaderStrategy</code>.
   * @param service The <code>TaskService</code> from which to obtain class
   *     definitions.
   * @param jobId The <code>UUID</code> identifying the job for which to
   *     obtain class definitions.
   * @param directory The root directory in which to store cached class
   *     definitions.
   * @throws IllegalArgumentException if <code>directory</code> does not
   *     refer to an existing directory.
   */
  public FileCachingJobServiceClassLoaderStrategy(TaskService service,
      UUID jobId, File directory) {
    super(service, jobId);
    this.directory = directory;
    if (!directory.isDirectory()) {
      throw new IllegalArgumentException("directory must be a directory.");
    }
  }

  /**
   * Creates a new <code>FileCachingJobServiceClassLoaderStrategy</code>.
   * @param service The <code>JobService</code> from which to obtain class
   *     definitions.
   * @param jobId The <code>UUID</code> identifying the job for which to
   *     obtain class definitions.
   * @param directory The root directory in which to store cached class
   *     definitions.
   * @throws IllegalArgumentException if <code>directory</code> does not
   *     refer to an existing directory.
   */
  public FileCachingJobServiceClassLoaderStrategy(JobService service,
      UUID jobId, String directory) {
    this(service, jobId, new File(directory));
  }

  @Override
  protected byte[] cacheLookup(String name, byte[] digest) {
    File file = getCacheEntryFile(name, digest, false);
    byte[] def = null;

    if (file.exists()) {
      try {
        def = FileUtil.getFileContents(file);
      } catch (IOException e) {
        logger.error("Could not read class definition file", e);
        def = null;
      }
    }

    return def;
  }

  @Override
  protected void cacheStore(String name, byte[] digest, byte[] def) {
    File file = getCacheEntryFile(name, digest, true);
    try {
      FileUtil.setFileContents(file, def);
    } catch (IOException e) {
      logger.error("Could not write class definition file", e);
    }
  }

  /**
   * Gets the <code>File</code> in which to store the given class definition.
   * @param name The fully qualified name of the class.
   * @param digest The MD5 digest of the class definition.
   * @param createDirectory A value indicating whether the directory
   *     containing the file should be created if it does not yet exist.
   * @return The <code>File</code> to use for storing the cached class
   *     definition.
   */
  private File getCacheEntryFile(String name, byte[] digest, boolean createDirectory) {
    File entryDirectory = new File(directory, name.replace('.', '/'));
    if (createDirectory && !entryDirectory.isDirectory()) {
      entryDirectory.mkdirs();
    }
    return new File(entryDirectory, StringUtil.toHex(digest));
  }

}
