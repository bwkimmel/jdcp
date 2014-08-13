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

/**
 * Indicates the state of a job hosted on the server.
 * @author Brad Kimmel
 */
public enum JobState {

	/** The job has been created but has not yet begun. */
	NEW,
	
	/** The job is being processed. */
	RUNNING,
	
	/**
	 * The job has stalled.  That is, results have been submitted for all
	 * pending tasks, but the job is not complete
	 * ({@link ca.eandb.jdcp.job.ParallelizableJob#isComplete()} returns
	 * <code>false</code>) and the job is not creating new tasks
	 * ({@link ca.eandb.jdcp.job.ParallelizableJob#getNextTask()} returns
	 * <code>null</code>).
	 */
	STALLED,
	
	/** The job is complete. */
	COMPLETE,
	
	/** The job has been cancelled. */
	CANCELLED
	
}
