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

package ca.eandb.jdcp.hub;

import java.rmi.RemoteException;
import java.util.UUID;

import org.apache.log4j.Logger;

import ca.eandb.jdcp.remote.JobService;
import ca.eandb.util.rmi.Serialized;

/**
 * @author Brad
 *
 */
final class TaskInfo {

	private static final Logger logger = Logger.getLogger(TaskInfo.class);

	private final JobService service;
	private final UUID jobId;
	private final int taskId;
	private boolean complete = false;

	public TaskInfo(UUID jobId, int taskId, JobService service) {
		this.jobId = jobId;
		this.taskId = taskId;
		this.service = service;
	}

	public UUID getJobId() {
		return jobId;
	}

	public int getTaskId() {
		return taskId;
	}

	public synchronized void submitTaskResults(Serialized<Object> results) {
		if (!complete) {
			try {
				service.submitTaskResults(jobId, taskId, results);
				complete = true;
			} catch (SecurityException e) {
				logger.error("Cannot submit task results", e);
			} catch (RemoteException e) {
				logger.error("Cannot submit task results", e);
			}
		}
	}

	public void reportException(Exception e) {
		if (!complete) {
			try {
				service.reportException(jobId, taskId, e);
			} catch (SecurityException e1) {
				logger.error("Could not report exception", e);
			} catch (RemoteException e1) {
				logger.error("Could not report exception", e);
			}
		}
	}

	public void setComplete() {
		complete = true;
	}

	public boolean isComplete() {
		return complete;
	}

}
