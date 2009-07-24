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

import java.nio.ByteBuffer;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import ca.eandb.jdcp.job.TaskWorker;
import ca.eandb.jdcp.remote.JobService;
import ca.eandb.jdcp.worker.CachingJobServiceClassLoaderStrategy;
import ca.eandb.jdcp.worker.DbCachingJobServiceClassLoaderStrategy;
import ca.eandb.util.rmi.Serialized;

/**
 * @author Brad
 *
 */
final class JobInfo {

	private static final Logger logger = Logger.getLogger(JobInfo.class);

	private final UUID id;
	private final JobService service;
	private final Map<Integer, TaskInfo> tasks = new HashMap<Integer, TaskInfo>();
	private Serialized<TaskWorker> worker = null;
	private final CachingJobServiceClassLoaderStrategy classCache;

	public JobInfo(UUID id, JobService service, DataSource dataSource) {
		this.id = id;
		this.service = service;
		this.classCache = new DbCachingJobServiceClassLoaderStrategy(service, id, dataSource);
	}

	public static void prepareDataSource(DataSource ds) throws SQLException {
		DbCachingJobServiceClassLoaderStrategy.prepareDataSource(ds);
	}

	public byte[] getClassDigest(String name) {
		return classCache.getClassDigest(name);
	}

	public byte[] getClassDefinition(String name) {
		ByteBuffer buf = classCache.getClassDefinition(name);
		return (buf != null) ? buf.array() : null;
	}

	public synchronized Serialized<TaskWorker> getTaskWorker() {
		if (worker == null) {
			try {
				worker = service.getTaskWorker(id);
			} catch (IllegalArgumentException e) {
				logger.error("Could not get task worker", e);
			} catch (SecurityException e) {
				logger.error("Could not get task worker", e);
			} catch (RemoteException e) {
				logger.error("Could not get task worker", e);
			}
		}
		return worker;
	}

	public void submitTaskResults(int taskId, Serialized<Object> results) {
		TaskInfo task = tasks.get(taskId);
		if (task != null) {
			task.submitTaskResults(results);
		}
	}

	public void reportException(int taskId, Exception e) {
		TaskInfo task = tasks.get(taskId);
		if (task != null) {
			task.reportException(e);
		}
	}

	public boolean isTaskComplete(int taskId) {
		TaskInfo task = tasks.get(taskId);
		return (task == null) || task.isComplete();
	}

	public void registerTask(int taskId) {
		TaskInfo task = new TaskInfo(id, taskId, service);
		tasks.put(taskId, task);
	}

	public synchronized void getIncompleteTasks(Collection<UUID> jobIds, Collection<Integer> taskIds) {
		for (TaskInfo task : tasks.values()) {
			if (!task.isComplete()) {
				jobIds.add(id);
				taskIds.add(task.getTaskId());
			}
		}
	}

	public void setTaskComplete(int taskId) {
		TaskInfo task = tasks.get(taskId);
		if (task != null) {
			task.setComplete();
		}
	}

}
