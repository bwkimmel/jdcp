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

	/** The inner <code>ParallelizableJob</code>. */
	private final ParallelizableJob job;

	/**
	 * Creates a new <code>JobExecutionWrapper</code>.
	 * @param job The inner <code>ParallelizableJob</code>.
	 */
	public JobExecutionWrapper(ParallelizableJob job) {
		this.job = job;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.ParallelizableJob#setHostService(ca.eandb.jdcp.job.HostService)
	 */
	public void setHostService(HostService host) {
		job.setHostService(host);
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.ParallelizableJob#finish()
	 */
	public void finish() throws JobExecutionException {
		try {
			job.finish();
		} catch (Exception e) {
			throw new JobExecutionException(e);
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.ParallelizableJob#getNextTask()
	 */
	public Object getNextTask() throws JobExecutionException {
		try {
			return job.getNextTask();
		} catch (Exception e) {
			throw new JobExecutionException(e);
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.ParallelizableJob#initialize(ca.eandb.jdcp.job.Host)
	 */
	public void initialize() throws JobExecutionException {
		try {
			job.initialize();
		} catch (Exception e) {
			throw new JobExecutionException(e);
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.ParallelizableJob#suspend()
	 */
	public void suspend() throws JobExecutionException {
		try {
			job.suspend();
		} catch (Exception e) {
			throw new JobExecutionException(e);
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.ParallelizableJob#resume()
	 */
	public void resume() throws JobExecutionException {
		try {
			job.resume();
		} catch (Exception e) {
			throw new JobExecutionException(e);
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.ParallelizableJob#isComplete()
	 */
	public boolean isComplete() throws JobExecutionException {
		try {
			return job.isComplete();
		} catch (Exception e) {
			throw new JobExecutionException(e);
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.ParallelizableJob#submitTaskResults(java.lang.Object, java.lang.Object, ca.eandb.util.progress.ProgressMonitor)
	 */
	public void submitTaskResults(Object task, Object results,
			ProgressMonitor monitor) throws JobExecutionException {
		try {
			job.submitTaskResults(task, results, monitor);
		} catch (Exception e) {
			throw new JobExecutionException(e);
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.ParallelizableJob#worker()
	 */
	public TaskWorker worker() throws JobExecutionException {
		try {
			return job.worker();
		} catch (Exception e) {
			throw new JobExecutionException(e);
		}
	}

}
