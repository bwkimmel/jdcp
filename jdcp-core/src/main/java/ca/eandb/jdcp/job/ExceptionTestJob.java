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

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;

import ca.eandb.util.io.Archive;
import ca.eandb.util.progress.ProgressMonitor;

/**
 * A <code>ParallelizableJob</code> to test handling of exceptions.
 * @author Brad Kimmel
 */
public final class ExceptionTestJob extends AbstractParallelizableJob implements Serializable {

  /** Serialization version ID. */
  private static final long serialVersionUID = 1608411874454559481L;

  /** The <code>TaskWorker</code> for the job. */
  private final TaskWorker worker;

  /** The index of the next task to be assigned. */
  private transient int nextTask = 0;

  /** The number of tasks complete. */
  private transient int tasksComplete = 0;

  /**
   * Creates a new <code>DiagnosticJob</code>.
   */
  public ExceptionTestJob() {
    this.worker = new ExceptionTaskWorker();
  }

  @Override
  public Object getNextTask() {
    return nextTask < 10 ? nextTask++ : null;
  }

  @Override
  public boolean isComplete() {
    return nextTask >= 10 && tasksComplete == nextTask;
  }

  @Override
  public void submitTaskResults(Object task, Object results,
      ProgressMonitor monitor) {

    System.out.print("Received results for task: ");
    System.out.println(task);

    System.out.print("Results: ");
    System.out.println(results);

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    monitor.notifyProgress(++tasksComplete, 10);

  }

  @Override
  public TaskWorker worker() {
    return this.worker;
  }

  @Override
  public void finish() throws IOException {
    PrintStream out = new PrintStream(createFileOutputStream("output.txt"));
    out.println("Done");
    out.flush();
    out.close();
  }

  @Override
  protected void archiveState(Archive ar) throws IOException {
    nextTask = ar.archiveInt(nextTask);
    tasksComplete = ar.archiveInt(tasksComplete);
  }

  /**
   * A <code>TaskWorker</code> for an <code>ExceptionTestJob</code>.
   * @author Brad Kimmel
   */
  private static final class ExceptionTaskWorker implements TaskWorker {

    /** Serialization version ID. */
    private static final long serialVersionUID = 1087328238920308359L;

    @Override
    public Object performTask(Object task, ProgressMonitor monitor) {
      throw new RuntimeException("Throwing an exception...");
    }

  }

}
