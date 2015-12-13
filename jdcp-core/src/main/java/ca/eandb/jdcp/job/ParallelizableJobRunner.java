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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import ca.eandb.util.UnexpectedException;
import ca.eandb.util.concurrent.BackgroundThreadFactory;
import ca.eandb.util.progress.DummyProgressMonitor;
import ca.eandb.util.progress.DummyProgressMonitorFactory;
import ca.eandb.util.progress.PermanentProgressMonitor;
import ca.eandb.util.progress.ProgressMonitor;
import ca.eandb.util.progress.ProgressMonitorFactory;

/**
 * A <code>Job</code> that runs a <code>ParallelizableJob</code> using multiple
 * threads.
 * @author Brad Kimmel
 */
public final class ParallelizableJobRunner implements Runnable {

  /**
   * Creates a new Builder for initializing a ParallelizableJobRunner.
   * @return The new <code>Builder</code>.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /** A Builder for creating ParallelizableJobRunner instances. */
  public static class Builder {
    private ParallelizableJob job = null;
    private File workingDirectory = null;
    private Executor executor = null;
    private int maxConcurrentWorkers =
        Runtime.getRuntime().availableProcessors();
    private ProgressMonitorFactory progressMonitorFactory
        = DummyProgressMonitorFactory.getInstance();
    private ProgressMonitor progressMonitor
        = DummyProgressMonitor.getInstance();

    /**
     * Must be created using static factory method.
     * @see ParallelizableJobRunner#newBuilder()
     */
    private Builder() {}

    /**
     * Creates the configured ParallelizableJobRunner instance.
     * @return The new <code>ParallelizableJobRunner</code>.
     * @throws IllegalStateException If the job has not been set.
     * @throws IOException If an error occurs setting up the working directory.
     * @see #setJob(ParallelizableJob)
     */
    public ParallelizableJobRunner build() throws IOException {
      if (job == null) {
        throw new IllegalStateException(
            "Cannot build ParallelizableJobRunner without a job.");
      }
      if (executor == null) {
        executor = Executors.newFixedThreadPool(
            maxConcurrentWorkers, new BackgroundThreadFactory());
      }
      return new ParallelizableJobRunner(job, workingDirectory, executor,
          maxConcurrentWorkers, progressMonitorFactory, progressMonitor);
    }

    /**
     * Sets the job to be executed by the ParallelizableJobRunner.  This
     * property is required in order to build the ParallelizableJobRunner.
     * @param job The <code>ParallelizableJob</code> to run.
     * @return This Builder.
     */
    public Builder setJob(ParallelizableJob job) {
      this.job = job;
      return this;
    }

    /**
     * Sets the working directory to use for writing results.  If not specified,
     * a temporary working directory will be used.
     * @param workingDirectory The working directory to write results to.
     * @return This Builder.
     */
    public Builder setWorkingDirectory(File workingDirectory) {
      this.workingDirectory = workingDirectory;
      return this;
    }

    /**
     * Sets the working directory to use for writing results.  If not specified,
     * a temporary working directory will be used.
     * @param workingDirectory The working directory to write results to.
     * @return This Builder.
     */
    public Builder setWorkingDirectory(String workingDirectory) {
      return setWorkingDirectory(new File(workingDirectory));
    }

    /**
     * Sets the executor to use to run the threads for processing individual
     * tasks.  If not set, a new fixed thread pool having maxConcurrentWorkers
     * threads will be created.
     * @param executor The Executor to use to run worker threads.
     * @return This Builder.
     */
    public Builder setExecutor(Executor executor) {
      this.executor = executor;
      return this;
    }

    /**
     * Sets the maximum number of concurrent tasks to run.  If not specified,
     * this will be set to <code>Runtime.getRuntime().availableProcessors()</code>.
     * @param maxConcurrentWorkers The maximum number of concurrent workers.
     * @return This Builder.
     * @see Runtime#availableProcessors()
     */
    public Builder setMaxConcurrentWorkers(int maxConcurrentWorkers) {
      this.maxConcurrentWorkers = maxConcurrentWorkers;
      return this;
    }

    /**
     * Sets the ProgressMonitorFactory to use to create ProgressMonitors for
     * individual tasks.  If not specified, a DummyProgressMonitorFactory will
     * be used.
     * @param progressMonitorFactory The ProgressMonitorFactory to use to create
     *     ProgressMonitors for individual tasks.
     * @return This Builder.
     * @see DummyProgressMonitorFactory
     */
    public Builder setProgressMonitorFactory(
        ProgressMonitorFactory progressMonitorFactory) {
      this.progressMonitorFactory = progressMonitorFactory;
      return this;
    }

    /**
     * Sets the ProgressMonitor to use to report overall progress to.  If not
     * specified, a DummyProgressMonitor will be used.
     * @param progressMonitor The ProgressMonitor to report overall progress to.
     * @return This Builder.
     * @see DummyProgressMonitor
     */
    public Builder setProgressMonitor(ProgressMonitor progressMonitor) {
      this.progressMonitor = progressMonitor;
      return this;
    }
  }

  /**
   * Creates a new <code>ParallelizableJobRunner</code>.
   * @param job The <code>ParallelizableJob</code> to run.
   * @param workingDirectory The working directory for the job.
   * @param executor The <code>Executor</code> to use to run worker threads.
   * @param maxConcurrentWorkers The maximum number of concurrent tasks to
   *     process.
   * @param monitorFactory The <code>ProgressMonitorFactory</code> to use to
   *     create <code>ProgressMonitor</code>s for worker tasks.
   * @param monitor The <code>ProgressMonitor</code> to report overall job
   *     progress to.
   */
  public ParallelizableJobRunner(ParallelizableJob job, File workingDirectory, Executor executor, int maxConcurrentWorkers, ProgressMonitorFactory monitorFactory, ProgressMonitor monitor) {
    this.job = new JobExecutionWrapper(job);
    this.workingDirectory = workingDirectory;
    this.executor = executor;
    this.workerSlot = new Semaphore(maxConcurrentWorkers);
    this.maxConcurrentWorkers = maxConcurrentWorkers;
    this.monitorFactory = monitorFactory;
    this.monitor = monitor;
  }

  /**
   * Creates a new <code>ParallelizableJobRunner</code>.
   * @param job The <code>ParallelizableJob</code> to run.
   * @param workingDirectory The working directory for the job.
   * @param maxConcurrentWorkers The maximum number of concurrent tasks to
   *     process.
   * @param monitorFactory The <code>ProgressMonitorFactory</code> to use to
   *     create <code>ProgressMonitor</code>s for worker tasks.
   * @param monitor The <code>ProgressMonitor</code> to report overall job
   *     progress to.
   */
  public ParallelizableJobRunner(ParallelizableJob job, File workingDirectory, int maxConcurrentWorkers, ProgressMonitorFactory monitorFactory, ProgressMonitor monitor) {
    this(job, workingDirectory, Executors.newFixedThreadPool(maxConcurrentWorkers, new BackgroundThreadFactory()), maxConcurrentWorkers, monitorFactory, monitor);
  }

  /**
   * Creates a new <code>ParallelizableJobRunner</code>.
   * @param job The <code>ParallelizableJob</code> to run.
   * @param workingDirectory The working directory for the job.
   * @param executor The <code>Executor</code> to use to run worker threads.
   * @param maxConcurrentWorkers The maximum number of concurrent tasks to
   *     process.
   */
  public ParallelizableJobRunner(ParallelizableJob job, File workingDirectory, Executor executor, int maxConcurrentWorkers) {
    this(job, workingDirectory, executor, maxConcurrentWorkers, DummyProgressMonitorFactory.getInstance(), DummyProgressMonitor.getInstance());
  }

  /**
   * Creates a new <code>ParallelizableJobRunner</code>.
   * @param job The <code>ParallelizableJob</code> to run.
   * @param workingDirectory The working directory for the job.
   * @param executor The <code>Executor</code> to use to run worker threads.
   * @param maxConcurrentWorkers The maximum number of concurrent tasks to
   *     process.
   */
  public ParallelizableJobRunner(ParallelizableJob job, String workingDirectory, Executor executor, int maxConcurrentWorkers) {
    this(job, new File(workingDirectory), executor, maxConcurrentWorkers);
  }

  /**
   * Creates a new <code>ParallelizableJobRunner</code>.
   * @param job The <code>ParallelizableJob</code> to run.
   * @param workingDirectory The working directory for the job.
   * @param maxConcurrentWorkers The maximum number of concurrent tasks to
   *     process.
   */
  public ParallelizableJobRunner(ParallelizableJob job, File workingDirectory, int maxConcurrentWorkers) {
    this(job, workingDirectory, Executors.newFixedThreadPool(maxConcurrentWorkers, new BackgroundThreadFactory()), maxConcurrentWorkers);
  }

  /**
   * Creates a new <code>ParallelizableJobRunner</code>.
   * @param job The <code>ParallelizableJob</code> to run.
   * @param workingDirectory The working directory for the job.
   * @param maxConcurrentWorkers The maximum number of concurrent tasks to
   *     process.
   */
  public ParallelizableJobRunner(ParallelizableJob job, String workingDirectory, int maxConcurrentWorkers) {
    this(job, new File(workingDirectory), maxConcurrentWorkers);
  }

  @Override
  public synchronized void run() {

    int taskNumber = 0;
    boolean complete = false;

    try {

      TaskWorker taskWorker = job.worker();
      this.job.setHostService(host);
      this.job.initialize();

      /* Task loop. */
      while (!this.monitor.isCancelPending()) {

        try {

          /* Acquire one of the slots for processing a task -- this
           * limits the processing to the specified number of concurrent
           * tasks.
           */
          this.workerSlot.acquire();
          if (workerException != null) {
            throw workerException;
          }

          /* Get the next task to run.  If there are no further tasks,
           * then wait for the remaining tasks to finish.
           */
          Object task = this.job.getNextTask();
          if (task == null) {
            this.workerSlot.acquire(this.maxConcurrentWorkers - 1);
            if (workerException != null) {
              throw workerException;
            }
            complete = true;
            break;
          }

          /* Create a worker and process the task. */
          Worker worker = new Worker(taskWorker, task, getWorkerProgressMonitor());

          notifyStatusChanged(String.format("Starting worker %d", ++taskNumber));
          this.executor.execute(worker);

        } catch (InterruptedException e) {
          e.printStackTrace();
        }

      }

      this.job.finish();

    } catch (JobExecutionException e) {

      throw new RuntimeException(e);

    }

    if (!complete) {
      monitor.notifyCancelled();
    } else {
      monitor.notifyComplete();
    }

  }

  /**
   * Gets an available <code>ProgressMonitor</code> to use for the next task.
   * @return An available <code>ProgressMonitor</code>.
   */
  private synchronized ProgressMonitor getWorkerProgressMonitor() {
    ProgressMonitor monitor;
    if (numProgressMonitors < maxConcurrentWorkers) {
      String title = String.format("Worker (%d)", numProgressMonitors++);
      monitor = new PermanentProgressMonitor(monitorFactory.createProgressMonitor(title));
    } else {
      monitor = workerMonitorQueue.remove();
    }
    return monitor;
  }

  /**
   * Notifies the progress monitor that the status has changed.
   * @param status A <code>String</code> describing the status.
   */
  private void notifyStatusChanged(String status) {
    synchronized (monitor) {
      this.monitor.notifyStatusChanged(status);
    }
  }

  /**
   * Submits results for a task.
   * @param task An <code>Object</code> describing the task for which results
   *     are being submitted.
   * @param results An <code>Object</code> describing the results.
   * @throws JobExecutionException
   */
  private void submitResults(Object task, Object results) throws JobExecutionException {
    synchronized (monitor) {
      this.job.submitTaskResults(task, results, monitor);
    }
  }

  private void setWorkerException(JobExecutionException e) {
    synchronized (monitor) {
      if (workerException == null) {
        workerException = e;
      }
    }
  }

  /**
   * Processes tasks for a <code>ParallelizableJob</code>.
   * @author Brad Kimmel
   */
  private class Worker implements Runnable {

    /**
     * Creates a new <code>Worker</code>.
     * @param task An <code>Object</code> describing the task to be
     *     processed by the worker.
     * @param monitor The <code>ProgressMonitor</code> to report progress
     *     to.
     */
    public Worker(TaskWorker worker, Object task, ProgressMonitor monitor) {
      this.worker = worker;
      this.task = task;
      this.monitor = monitor;
    }

    @Override
    public void run() {
      try {
        submitResults(task, worker.performTask(task, monitor));
      } catch (JobExecutionException e) {
        setWorkerException(e);
      } catch (Exception e) {
        setWorkerException(new JobExecutionException(e));
      } finally {
        workerMonitorQueue.add(monitor);
        workerSlot.release();
      }
    }

    /** The <code>ProgressMonitor</code> to report progress to. */
    private final ProgressMonitor monitor;

    /** An <code>Object</code> describing the task to be processed. */
    private final Object task;

    /** The <code>TaskWorker</code> to use to perform the task. */
    private final TaskWorker worker;

  }

  /**
   * Gets the working directory, creating a temporary one if one was not
   * specified during construction.
   * @return The working directory.
   */
  private synchronized File getWorkingDirectory() {
    if (workingDirectory == null) {
      try {
        workingDirectory =
            Files.createTempDirectory(TEMP_DIRECTORY_PREFIX).toFile();
      } catch (IOException e) {
        throw new UnexpectedException(e);
      }
    }
    return workingDirectory;
  }

  private final HostService host = new HostService() {

    public FileOutputStream createFileOutputStream(String path) {
      File file = new File(getWorkingDirectory(), path);
      File directory = file.getParentFile();
      directory.mkdirs();
      try {
        return new FileOutputStream(file);
      } catch (FileNotFoundException e) {
        throw new UnexpectedException(e);
      }
    }

    public RandomAccessFile createRandomAccessFile(String path) {
      File file = new File(getWorkingDirectory(), path);
      File directory = file.getParentFile();
      directory.mkdirs();
      try {
        return new RandomAccessFile(file, "rw");
      } catch (FileNotFoundException e) {
        throw new UnexpectedException(e);
      }
    }

  };

  /** The prefix to use for temporary working directories. */
  private static final String TEMP_DIRECTORY_PREFIX = "jdcp-";

  /**
   * The <code>ProgressMonitorFactory</code> to use to create
   * <code>ProgressMonitor</code>s for worker tasks.
   */
  private final ProgressMonitorFactory monitorFactory;

  /** The <code>ParallelizableJob</code> to be run. */
  private final JobExecutionWrapper job;

  /** The working directory for this job. */
  private File workingDirectory;

  /**
   * The <code>Semaphore</code> to use to limit the number of concurrent
   * threads.
   */
  private final Semaphore workerSlot;

  /** The <code>Executor</code> to use to run worker threads. */
  private final Executor executor;

  /** The maximum number of concurrent tasks to process. */
  private final int maxConcurrentWorkers;

  /** The <code>Queue</code> of <code>ProgressMonitor</code>s for workers. */
  private final Queue<ProgressMonitor> workerMonitorQueue = new ConcurrentLinkedQueue<ProgressMonitor>();

  /** The number of child <code>ProgressMonitor</code>s created. */
  private int numProgressMonitors = 0;

  /** The current <code>ProgressMonitor</code>. */
  private ProgressMonitor monitor;

  /** An exception that occurred while processing a task. */
  private JobExecutionException workerException = null;

}
