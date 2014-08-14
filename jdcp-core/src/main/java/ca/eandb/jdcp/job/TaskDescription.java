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

import java.io.Serializable;
import java.util.UUID;


import ca.eandb.jdcp.remote.JobService;
import ca.eandb.util.rmi.Serialized;

/**
 * A description of a task assigned by a <code>JobMasterService</code>.
 * @author Brad Kimmel
 * @see {@link JobService#requestTask()}.
 */
public final class TaskDescription implements Serializable {

  /**
   * Initializes the task description.
   * @param jobId The <code>UUID</code> of the job that the task is for.
   * @param taskId The ID of the task to be performed.
   * @param task An <code>Object</code> describing the task to be performed.
   *     This should be passed to <code>TaskWorker.performTask</code>.
   * @see {@link #performTask(Object, ca.eandb.util.progress.ProgressMonitor)}.
   */
  public TaskDescription(UUID jobId, int taskId, Object task) {
    this.jobId = jobId;
    this.taskId = taskId;
    this.task = new Serialized<Object>(task);
  }

  /**
   * Gets the <code>Object</code> describing the task to be performed.  This
   * should be passed to <code>TaskWorker.performTask</code> for the
   * <code>TaskWorker</code> corresponding to the job with the
   * <code>UUID</code> given by {@link #getJobId()}.  The <code>TaskWorker</code>
   * may be obtained by calling {@link JobService#getTaskWorker(UUID)}.
   * @return The <code>Object</code> describing the task to be performed.
   * @see {@link #getJobId()},
   *     {@link #performTask(Object, ca.eandb.util.progress.ProgressMonitor)},
   *     {@link JobService#getTaskWorker(UUID)}.
   */
  public Serialized<Object> getTask() {
    return this.task;
  }

  /**
   * Gets the <code>UUID</code> of the job whose <code>TaskWorker</code>
   * should perform this task.  Call {@link JobService#getTaskWorker(UUID)}
   * to get the <code>TaskWorker</code> to use to perform this task.
   * @return The <code>UUID</code> of the job that this task is associated
   *     with.
   * @see {@link JobService#getTaskWorker(UUID)}.
   */
  public UUID getJobId() {
    return this.jobId;
  }

  /**
   * The ID of the task to be performed.  This should be passed back to
   * <code>JobMasterService.submitTaskResults</code> when submitting the
   * results of this task.
   * @return The ID of the task to be performed.
   * @see {@link JobService#submitTaskResults(UUID, int, Object)}.
   */
  public int getTaskId() {
    return this.taskId;
  }

  /** The <code>UUID</code> of the job that this task is a part of. */
  private final UUID jobId;

  /** The ID of this task. */
  private final int taskId;

  /** The <code>Object</code> describing the task to be performed. */
  private final Serialized<Object> task;

  /**
   * Serialization version ID.
   */
  private static final long serialVersionUID = 295569474645825592L;

}
