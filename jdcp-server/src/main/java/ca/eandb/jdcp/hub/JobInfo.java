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
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;

import javax.sql.DataSource;

import ca.eandb.jdcp.job.TaskWorker;
import ca.eandb.jdcp.worker.CachingJobServiceClassLoaderStrategy;
import ca.eandb.jdcp.worker.DbCachingJobServiceClassLoaderStrategy;
import ca.eandb.util.rmi.Serialized;

/**
 * @author Brad
 *
 */
final class JobInfo {

	private final UUID id;
	private final ServiceWrapper service;
	private final Set<Integer> activeTaskIds = new HashSet<Integer>();
	private Serialized<TaskWorker> worker = null;
	private final CachingJobServiceClassLoaderStrategy classCache;

	public JobInfo(UUID id, ServiceWrapper service, DataSource dataSource, Executor executor) {
		this.id = id;
		this.service = service;
		this.classCache = new DbCachingJobServiceClassLoaderStrategy(service, id, dataSource);

		initTaskWorker(executor);
	}

	public static void prepareDataSource(DataSource ds) throws SQLException {
		DbCachingJobServiceClassLoaderStrategy.prepareDataSource(ds);
	}

	private void initTaskWorker(Executor executor) {
		executor.execute(new Runnable() {
			public void run() {
				getTaskWorker();
			}
		});
	}

	public UUID getJobId() {
		return id;
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
			worker = service.getTaskWorker(id);
		}
		return worker;
	}

	public void submitTaskResults(int taskId, Serialized<Object> results) {
		service.submitTaskResults(id, taskId, results);
		activeTaskIds.remove(taskId);
	}

	public void reportException(int taskId, Exception e) {
		service.reportException(id, taskId, e);
	}

	public boolean isTaskComplete(int taskId) {
		return !activeTaskIds.contains(taskId);
	}

	public void registerTask(int taskId) {
		activeTaskIds.add(taskId);
	}

	public void removeTask(int taskId) {
		activeTaskIds.remove(taskId);
	}

	public Set<Integer> getActiveTasks() {
		return activeTaskIds;
	}

}
