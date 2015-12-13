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

import java.io.ObjectInput;
import java.io.ObjectOutput;

import ca.eandb.util.progress.ProgressMonitor;

/**
 * A <code>ParallelizableJob</code> decorator that wraps exceptions thrown by
 * the inner <code>ParallelizableJob</code> in a
 * <code>JobExecutionException</code>.
 * @author Brad Kimmel
 * @see ca.eandb.jdcp.job.ParallelizableJob
 * @see ca.eandb.jdcp.job.JobExecutionException
 */
public final class JobExecutionWrapper implements ParallelizableJob {

  /**
   * Serialization version ID.
   */
  private static final long serialVersionUID = -3231530847968982289L;

  /** The inner <code>ParallelizableJob</code>. */
  private final ParallelizableJob job;

  /**
   * Creates a new <code>JobExecutionWrapper</code>.
   * @param job The inner <code>ParallelizableJob</code>.
   */
  public JobExecutionWrapper(ParallelizableJob job) {
    this.job = job;
  }

  @Override
  public void setHostService(HostService host) {
    job.setHostService(host);
  }

  @Override
  public void finish() throws JobExecutionException {
    try {
      job.finish();
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }
  }

  @Override
  public Object getNextTask() throws JobExecutionException {
    try {
      return job.getNextTask();
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }
  }

  @Override
  public void initialize() throws JobExecutionException {
    try {
      job.initialize();
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }
  }

  @Override
  public void saveState(ObjectOutput output) throws JobExecutionException {
    try {
      job.saveState(output);
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }
  }

  @Override
  public void restoreState(ObjectInput input) throws JobExecutionException {
    try {
      job.restoreState(input);
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }
  }

  @Override
  public boolean isComplete() throws JobExecutionException {
    try {
      return job.isComplete();
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }
  }

  @Override
  public void submitTaskResults(Object task, Object results,
      ProgressMonitor monitor) throws JobExecutionException {
    try {
      job.submitTaskResults(task, results, monitor);
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }
  }

  @Override
  public TaskWorker worker() throws JobExecutionException {
    try {
      return new TaskWorkerWrapper(job.worker());
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }
  }

  private static final class TaskWorkerWrapper implements TaskWorker {

    /** Serialization version ID. */
    private static final long serialVersionUID = 997997810158070736L;

    private final TaskWorker inner;

    public TaskWorkerWrapper(TaskWorker inner) {
      this.inner = inner;
    }

    @Override
    public Object performTask(Object task, ProgressMonitor monitor)
        throws JobExecutionException {
      try {
        return inner.performTask(task, monitor);
      } catch (Exception e) {
        throw new JobExecutionException(e);
      }
    }

  }

}
