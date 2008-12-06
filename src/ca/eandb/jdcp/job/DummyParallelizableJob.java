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
import java.io.PrintStream;
import java.io.Serializable;

import ca.eandb.util.progress.ProgressMonitor;

/**
 * A dummy parallelizable job to test remote method invocation.
 * @author bkimmel
 */
public final class DummyParallelizableJob extends AbstractParallelizableJob
		implements Serializable {

	/**
	 * Initializes the number of tasks to serve and the amount of time to
	 * delay to simulate the processing of a task.
	 * @param tasks The number of tasks to serve.
	 * @param minSleepTime The minimum time (in milliseconds) to sleep to
	 * 		simulate the processing of a task.
	 * @param maxSleepTime The maximum time (in milliseconds) to sleep to
	 * 		simulate the processing of a task.
	 */
	public DummyParallelizableJob(int tasks, int minSleepTime, int maxSleepTime) {
		this.tasks = tasks;
		this.minSleepTime = minSleepTime;
		this.maxSleepTime = maxSleepTime;
		assert(minSleepTime <= maxSleepTime);
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.ParallelizableJob#getNextTask()
	 */
	public Object getNextTask() {

		if (this.nextTask < this.tasks) {

			System.out.printf("Task %d requested.\n", this.nextTask);
			return this.nextTask++;

		} else { /* this.nextTask >= this.tasks */

			System.out.println("No more tasks.");
			return null;

		}

	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.ParallelizableJob#submitResults(java.lang.Object, java.lang.Object, ca.eandb.util.progress.ProgressMonitor)
	 */
	public void submitTaskResults(Object task, Object results, ProgressMonitor monitor) {

		int taskValue = (Integer) task;
		int resultValue = (Integer) results;
		System.out.printf("Received results for task %d: %d.\n", taskValue, resultValue);

		monitor.notifyProgress(++this.numResultsReceived, this.tasks);

	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.ParallelizableJob#isComplete()
	 */
	public boolean isComplete() {
		return this.numResultsReceived >= this.tasks;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.ParallelizableJob#finish()
	 */
	public void finish() {

		FileOutputStream stream = createFileOutputStream("results.txt");
		PrintStream results = new PrintStream(stream);

		results.printf("DummyParallelizableJob complete (%d tasks).\n", this.tasks);
		results.flush();
		results.close();

	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.ParallelizableJob#worker()
	 */
	public TaskWorker worker() {
		return this.worker;
	}

	/**
	 * A random number generator.
	 */
	private static final java.util.Random random = new java.util.Random();

	/**
	 * The minimum time (in milliseconds) to sleep to simulate the processing
	 * of a task.
	 */
	private final int minSleepTime;

	/**
	 * The maximum time (in milliseconds) to sleep to simulate the processing
	 * of a task.
	 */
	private final int maxSleepTime;

	/** The number of tasks to serve. */
	private final int tasks;

	/** The index of the next task to serve. */
	private int nextTask = 0;

	/** The number of results that have been received. */
	private int numResultsReceived = 0;

	/**
	 * The task worker to use to process tasks.
	 */
	private final TaskWorker worker = new TaskWorker() {

		/*
		 * (non-Javadoc)
		 * @see ca.eandb.jdcp.job.TaskWorker#performTask(java.lang.Object, ca.eandb.util.progress.ProgressMonitor)
		 */
		public Object performTask(Object task, ProgressMonitor monitor) {

			int value = (Integer) task;
			String msg = String.format("Processing task %d.", value);

			monitor.notifyStatusChanged(msg);
			System.out.println(msg);

			int sleepTime = minSleepTime + random.nextInt(maxSleepTime - minSleepTime + 1);

			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				// nothing to do.
			}

			msg = String.format("Done task %d.", value);
			monitor.notifyStatusChanged(msg);
			System.out.println(msg);

			return value;

		}

		/** The serialization version ID. */
		private static final long serialVersionUID = -4687914341839279922L;

	};

	/**
	 * Serialization version ID.
	 */
	private static final long serialVersionUID = 4328712633325360415L;

}
