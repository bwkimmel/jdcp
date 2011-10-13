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
import java.util.Random;
import java.util.Stack;

import ca.eandb.util.progress.ProgressMonitor;

/**
 * A <code>ParallelizableJob</code> decorator that randomizes the order in
 * which its tasks are assigned.
 * @author Brad Kimmel
 */
public class TaskReversedJob implements ParallelizableJob {

	/** Serialization version ID. */
	private static final long serialVersionUID = -6185355912678518969L;

	/** The underlying <code>ParallelizableJob</code>. */
	private final ParallelizableJob inner;

	/** The list of unassigned tasks. */
	private final Stack<Object> tasks = new Stack<Object>();

	/**
	 * Creates a new <code>TaskRandomziedJob</code>.
	 * @param inner The <code>ParallelizableJob</code> whose tasks to execute
	 * 		in random order.
	 */
	public TaskReversedJob(ParallelizableJob inner) {
		this.inner = inner;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.ParallelizableJob#getNextTask()
	 */
	public Object getNextTask() throws Exception {
		while (true) {
			Object task = inner.getNextTask();
			if (task == null) {
				break;
			}
			tasks.push(task);
		}
		return tasks.isEmpty() ? null : tasks.pop();
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.ParallelizableJob#isComplete()
	 */
	public boolean isComplete() throws Exception {
		return inner.isComplete();
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.ParallelizableJob#submitTaskResults(java.lang.Object, java.lang.Object, ca.eandb.util.progress.ProgressMonitor)
	 */
	public void submitTaskResults(Object task, Object results,
			ProgressMonitor monitor) throws Exception {
		inner.submitTaskResults(task, results, monitor);
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.AbstractParallelizableJob#finish()
	 */
	public void finish() throws Exception {
		inner.finish();
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.AbstractParallelizableJob#initialize()
	 */
	public void initialize() throws Exception {
		inner.initialize();
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.AbstractParallelizableJob#restoreState(java.io.ObjectInput)
	 */
	public void restoreState(ObjectInput input) throws Exception {
		inner.restoreState(input);
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.AbstractParallelizableJob#saveState(java.io.ObjectOutput)
	 */
	public void saveState(ObjectOutput output) throws Exception {
		inner.saveState(output);
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.AbstractParallelizableJob#setHostService(ca.eandb.jdcp.job.HostService)
	 */
	public void setHostService(HostService host) {
		inner.setHostService(host);
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.ParallelizableJob#worker()
	 */
	public TaskWorker worker() throws Exception {
		return inner.worker();
	}

}
