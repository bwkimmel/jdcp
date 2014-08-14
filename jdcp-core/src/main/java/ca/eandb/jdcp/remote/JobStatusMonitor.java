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

package ca.eandb.jdcp.remote;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import ca.eandb.util.progress.ProgressMonitor;
import ca.eandb.util.progress.ProgressMonitorFactory;

/**
 * Processes <code>JobStatus</code> events and updates
 * <code>ProgressMonitor</code>s generated from a provided
 * <code>ProgressMonitorFactory</code> accordingly.
 *
 * @author Brad Kimmel
 */
public final class JobStatusMonitor {

  /**
   * The <code>ProgressMonitorFactory</code> to use to create the
   * <code>ProgressMonitor</code>s.
   */
  private final ProgressMonitorFactory factory;

  /** The internal objects for handling events for each job. */
  private final Map<UUID, JobMonitor> monitors = new HashMap<UUID, JobMonitor>();

  /**
   * Creates a <code>JobStatusMonitor</code>.
   * @param factory The <code>ProgressMonitorFactory</code> to use to create
   *     the <code>ProgressMonitor</code>s.
   */
  public JobStatusMonitor(ProgressMonitorFactory factory) {
    this.factory = factory;
  }

  /**
   * Gets the <code>JobMonitor</code> for the job that the specified
   * <code>JobStatus</code> event is for.
   * @param status The <code>JobStatus</code> event for which to obtain the
   *     corresponding <code>JobMonitor</code>.
   * @return The <code>JobMonitor</code>.
   */
  private synchronized JobMonitor getJobMonitor(JobStatus status) {
    UUID jobId = status.getJobId();
    JobMonitor monitor = monitors.get(jobId);
    if (monitor == null) {
      monitor = new JobMonitor(factory.createProgressMonitor(status.getDescription()));
      monitors.put(jobId, monitor);
    }
    return monitor;
  }

  /**
   * Updates the appropriate <code>ProgressMonitor</code> (creating one if
   * necessary) according to the new status of the job.
   * @param newStatus The <code>JobStatus</code> indicating the new status of
   *     the job.
   */
  public void updateStatus(JobStatus newStatus) {
    JobMonitor monitor = getJobMonitor(newStatus);
    monitor.updateStatus(newStatus);
  }

  /** Handles <code>JobStatus</code> events for a single job. */
  private static class JobMonitor {

    /** The <code>ProgressMonitor</code> to monitor changes for this job. */
    private final ProgressMonitor monitor;

    /** The most recent <code>JobStatus</code> for this job. */
    private JobStatus status = null;

    /**
     * Creates a new <code>JobMonitor</code>.
     * @param monitor The <code>ProgressMonitor</code> to monitor changes
     *     for this job.
     */
    public JobMonitor(ProgressMonitor monitor) {
      this.monitor = monitor;
    }

    /**
     * Updates the <code>ProgressMonitor</code> according to the new
     *     status of the job.
     * @param newStatus The <code>JobStatus</code> indicating the new
     *     status of the job.
     */
    public synchronized void updateStatus(JobStatus newStatus) {

      assert(status == null || newStatus.getJobId().equals(status.getJobId()));

      if (newStatus.isProgressIndeterminant()) {
        monitor.notifyIndeterminantProgress();
      } else if (status == null || newStatus.getProgress() != status.getProgress()) {
        monitor.notifyProgress(newStatus.getProgress());
      }

      if (status == null || !newStatus.getStatus().equals(status.getStatus())) {
        monitor.notifyStatusChanged(newStatus.getStatus());
      }

      if (newStatus.isComplete() && (status == null || !status.isComplete())) {
        monitor.notifyComplete();
      }

      if (newStatus.isCancelled() && (status == null || !status.isCancelled())) {
        monitor.notifyCancelled();
      }

      status = newStatus;

    }

  }

}
