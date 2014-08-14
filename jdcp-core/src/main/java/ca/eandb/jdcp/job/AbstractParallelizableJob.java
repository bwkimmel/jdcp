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
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.RandomAccessFile;

import ca.eandb.util.io.Archive;
import ca.eandb.util.io.InputArchive;
import ca.eandb.util.io.OutputArchive;

/**
 * An abstract <code>ParallelizableJob</code> that provides a default
 * implementation for the <code>Job</code> interface.
 * @author Brad Kimmel
 */
public abstract class AbstractParallelizableJob implements ParallelizableJob {

  /**
   * A <code>HostService</code> object for providing secure access to the
   * file system on the machine hosting this <code>ParallelizableJob</code>.
   */
  private transient HostService host = null;

  /* (non-Javadoc)
   * @see ca.eandb.jdcp.job.ParallelizableJob#setHostService(ca.eandb.jdcp.job.HostService)
   */
  public final void setHostService(HostService host) {
    this.host = host;
  }

  /* (non-Javadoc)
   * @see ca.eandb.jdcp.job.ParallelizableJob#initialize()
   */
  public void initialize() throws Exception {
    /* nothing to do. */
  }

  /* (non-Javadoc)
   * @see ca.eandb.jdcp.job.ParallelizableJob#finish()
   */
  public void finish() throws Exception {
    /* nothing to do. */
  }

  /* (non-Javadoc)
   * @see ca.eandb.jdcp.job.ParallelizableJob#restoreState(java.io.ObjectInput)
   */
  public void restoreState(ObjectInput input) throws Exception {
    archiveState(new InputArchive(input));
  }

  /* (non-Javadoc)
   * @see ca.eandb.jdcp.job.ParallelizableJob#saveState(java.io.ObjectOutput)
   */
  public void saveState(ObjectOutput output) throws Exception {
    archiveState(new OutputArchive(output));
  }

  /**
   * Saves or restores the state of this job from an archive.
   * @param ar The <code>Archive</code> to read from or write to.
   * @throws Exception If the operation fails.
   */
  protected void archiveState(Archive ar) throws Exception {
    throw new UnsupportedOperationException();
  }

  /**
   * Creates a <code>FileOutputStream</code> for a file in this job's
   * working directory.
   * @param name The relative path to the file.
   * @return The new <code>FileOutputStream</code>.
   * @see ca.eandb.jdcp.job.HostService#createFileOutputStream(java.lang.String)
   */
  protected FileOutputStream createFileOutputStream(String name) {
    return host.createFileOutputStream(name);
  }

  /**
   * Creates a new <code>RandomAccessFile</code> in this job's working
   * directory.
   * @param name The relative path to the new file.
   * @return The new <code>RandomAccessFile</code>.
   * @see ca.eandb.jdcp.job.HostService#createRandomAccessFile(java.lang.String)
   */
  protected RandomAccessFile createRandomAccessFile(String name) {
    return host.createRandomAccessFile(name);
  }

}
