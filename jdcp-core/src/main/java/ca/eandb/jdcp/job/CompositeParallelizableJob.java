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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import ca.eandb.util.progress.CancelListener;
import ca.eandb.util.progress.ProgressMonitor;

/**
 * A <code>ParallelizableJob</code> that combines multiple jobs into one.
 *
 * @author Brad Kimmel
 */
public final class CompositeParallelizableJob implements ParallelizableJob {

  /** Serialization version ID. */
  private static final long serialVersionUID = 2314913595545088051L;

  private final List<ParallelizableJob> jobs = new ArrayList<ParallelizableJob>();

  private transient TaskWorker worker = null;

  private transient double[] jobProgress = null;

  private transient double totalProgress = 0.0;

  public CompositeParallelizableJob() {
    /* nothing to do. */
  }

  public CompositeParallelizableJob(Collection<ParallelizableJob> jobs) {
    this.jobs.addAll(jobs);
  }

  public CompositeParallelizableJob addJob(ParallelizableJob job) {
    jobs.add(job);
    return this;
  }

  public CompositeParallelizableJob addJobs(Collection<ParallelizableJob> jobs) {
    this.jobs.addAll(jobs);
    return this;
  }

  private final class CompositeProgressMonitor implements ProgressMonitor {

    private final int jobNumber;

    public CompositeProgressMonitor(int jobNumber) {
      this.jobNumber = jobNumber;
    }

    @Override
    public boolean notifyProgress(int value, int maximum) {
      return notifyProgress((double) value / (double) maximum);
    }

    @Override
    public synchronized boolean notifyProgress(double progress) {
      if (jobProgress == null) {
        jobProgress = new double[jobs.size()];
      }
      totalProgress += (progress - jobProgress[jobNumber]) / jobs.size();
      jobProgress[jobNumber] = progress;
      return true;
    }

    @Override
    public boolean notifyIndeterminantProgress() {
      /* ignore. */
      return true;
    }

    @Override
    public void notifyComplete() {
      notifyProgress(1.0);
    }

    @Override
    public void notifyCancelled() {
      /* ignore. */
    }

    @Override
    public void notifyStatusChanged(String status) {
      /* ignore. */
    }

    @Override
    public boolean isCancelPending() {
      return false;
    }

    @Override
    public void addCancelListener(CancelListener listener) {
      /* nothing to do. */
    }

  }

  private static final class JobItem implements Serializable {

    /** Serialization version ID. */
    private static final long serialVersionUID = 3197453751237018890L;

    public final int jobNumber;
    public final Object item;

    public JobItem(int jobNumber, Object item) {
      this.jobNumber = jobNumber;
      this.item = item;
    }

  }

  private static final class CompositeTaskWorker implements TaskWorker {

    /** Serialization version ID. */
    private static final long serialVersionUID = -9185015559500365734L;

    private final TaskWorker[] workers;

    public CompositeTaskWorker(TaskWorker[] workers) {
      this.workers = workers;
    }

    @Override
    public Object performTask(Object task_, ProgressMonitor monitor)
        throws Exception {
      JobItem task = (JobItem) task_;
      return workers[task.jobNumber].performTask(task.item, monitor);
    }

  }

  @Override
  public void setHostService(HostService host) {
    for (ParallelizableJob job : jobs) {
      job.setHostService(host);
    }
  }

  @Override
  public void initialize() throws Exception {
    for (LifeCycleManageable job : jobs) {
      job.initialize();
    }
  }

  @Override
  public void finish() throws Exception {
    /* Nothing to do.  finish() is called on sub jobs as they complete. */
  }

  @Override
  public void saveState(ObjectOutput output) throws Exception {
    for (LifeCycleManageable job : jobs) {
      job.saveState(output);
    }
  }

  @Override
  public void restoreState(ObjectInput input) throws Exception {
    for (LifeCycleManageable job : jobs) {
      job.restoreState(input);
    }
  }

  @Override
  public Object getNextTask() throws Exception {
    for (int i = 0, n = jobs.size(); i < n; i++) {
      ParallelizableJob job = jobs.get(i);
      Object task = job.getNextTask();
      if (task != null) {
        return new JobItem(i, task);
      }
    }
    return null;
  }

  @Override
  public void submitTaskResults(Object task_, Object results,
      ProgressMonitor monitor) throws Exception {
    JobItem task = (JobItem) task_;
    ParallelizableJob job = jobs.get(task.jobNumber);
    job.submitTaskResults(task.item, results, new CompositeProgressMonitor(task.jobNumber));
    if (job.isComplete()) {
      job.finish();
    }
    monitor.notifyProgress(totalProgress);
  }

  @Override
  public boolean isComplete() throws Exception {
    for (ParallelizableJob job : jobs) {
      if (!job.isComplete()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public synchronized TaskWorker worker() throws Exception {
    if (worker == null) {
      int njobs = jobs.size();
      TaskWorker[] workers = new TaskWorker[njobs];
      for (int i = 0; i < njobs; i++) {
        workers[i] = jobs.get(i).worker();
      }
      worker = new CompositeTaskWorker(workers);
    }
    return worker;
  }

}
