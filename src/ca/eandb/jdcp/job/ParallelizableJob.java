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
 * Represents a job that can be split into smaller chunks.
 * @author Brad Kimmel
 */
public interface ParallelizableJob {

	/**
	 * Sets the <code>HostService</code> object that provides secure access
	 * to the file system on the machine hosting this
	 * <code>ParallelizableJob</code>.
	 * @param host The <code>HostService</code> to use.
	 */
	void setHostService(HostService host);

	/**
	 * Sets up this <code>ParallelizableJob</code> on the machine hosting the
	 * job (not necessarily the same machine as the one processing the tasks
	 * for this job).
	 */
	void initialize() throws Exception;

	/**
	 * Performs any final actions required for this
	 * <code>ParallelizableJob</code>.
	 */
	void finish() throws Exception;

	/**
	 * Notifies the job that it is about to be suspended (for example, if the
	 * server application is about to be shut down).
	 */
	void saveState(ObjectOutput output) throws Exception;

	/**
	 * Notifies the job that it is about to be resumed after having been
	 * suspended.  The job should reinstate any transient data (e.g., open
	 * files).
	 */
	void restoreState(ObjectInput input) throws Exception;

	/**
	 * Gets the next task to be performed.
	 * @return The <code>Object</code> describing the next task to be
	 * 		performed, or <code>null</code> if there are no remaining
	 * 		tasks.
	 */
	Object getNextTask() throws Exception;

	/**
	 * Submits the results of a task.
	 * @param task The <code>Object</code> describing the task for which
	 * 		results are being submitted (must have been obtained from a
	 * 		previous call to {@link #getNextTask()}.
	 * @param results The <code>Object</code> containing the results of
	 * 		a task.
	 * @param monitor The <code>ProgressMonitor</code> to update with the
	 * 		progress of this <code>Job</code>.
	 * @see {@link #getNextTask()}.
	 */
	void submitTaskResults(Object task, Object results, ProgressMonitor monitor) throws Exception;

	/**
	 * Gets a value that indicates if this job is complete (i.e., if results
	 * for all tasks have been submitted).
	 * @return A value indicating if this job is complete.
	 */
	boolean isComplete() throws Exception;

	/**
	 * Gets the task worker to use to process the tasks of this job.
	 * @return The task worker to use to process the tasks of this job.
	 */
	TaskWorker worker() throws Exception;

}
