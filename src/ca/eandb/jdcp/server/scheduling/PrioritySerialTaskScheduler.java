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

package ca.eandb.jdcp.server.scheduling;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.UUID;

import ca.eandb.jdcp.job.TaskDescription;
import ca.eandb.jdcp.remote.JobService;

/**
 * A <code>TaskScheduler</code> that serves tasks for the earliest scheduled
 * job having the highest priority in a round robin fashion.  That is, tasks
 * are scheduled so that each job completes before the next one starts.
 * @author Brad Kimmel
 */
public final class PrioritySerialTaskScheduler implements TaskScheduler {

	/**
	 * A <code>Map</code> associating information about a job with the
	 * corresponding job's <code>UUID</code>.
	 * @see JobInfo
	 */
	private Map<UUID, JobInfo> jobs = new HashMap<UUID, JobInfo>();

	/**
	 * A <code>PriorityQueue</code> used to determine which job is next in
	 * line.
	 */
	private PriorityQueue<UUID> jobQueue = new PriorityQueue<UUID>(11, new JobIdComparator());

	/**
	 * Each job is assigned an order number using an increasing counter.  This
	 * allows {@link #jobQueue} to determine in which order jobs were first
	 * seen.  This field stores the order number to assign to the next job that
	 * is added.
	 */
	private int nextOrder = 0;

	/**
	 * Represents bookkeeping information about a
	 * <code>ParallelizableJob</code>.
	 * @author Brad Kimmel
	 */
	private final class JobInfo implements Comparable<JobInfo> {

		/** The <code>UUID</code> for this job. */
		public final UUID id;

		/** The priority assigned to this job. */
		private int priority = JobService.DEFAULT_PRIORITY;

		/** The order in which this job was added to the schedule. */
		private final int order = nextOrder++;

		/**
		 * A <code>Map</code> associating task IDs with the corresponding
		 * <code>TaskDescription</code>.
		 */
		private Map<Integer, TaskDescription> tasks = new HashMap<Integer, TaskDescription>();

		/**
		 * A <code>LinkedList</code> of task IDs used to
		 */
		private final LinkedList<Integer> taskQueue = new LinkedList<Integer>();

		/**
		 * Creates a new <code>JobInfo</code>.
		 * @param id The <code>UUID</code> identifying the job that this
		 * 		<code>JobInfo</code> describes.
		 */
		public JobInfo(UUID id) {
			this.id = id;
		}

		/* (non-Javadoc)
		 * @see java.lang.Comparable#compareTo(java.lang.Object)
		 */
		public int compareTo(JobInfo other) {
			if (priority > other.priority) {
				return -1;
			} else if (priority < other.priority) {
				return 1;
			} else if (order < other.order) {
				return -1;
			} else if (order > other.order) {
				return 1;
			} else {
				return 0;
			}
		}

		/**
		 * Adds a task to the queue for this job.
		 * @param task The <code>Object</code> describing the task to be
		 * 		scheduled.
		 * @return The task ID for the newly scheduled task.
		 */
		public synchronized void addTask(TaskDescription task) {
			int taskId = task.getTaskId();
			tasks.put(taskId, task);
			taskQueue.addFirst(taskId);
		}

		/**
		 * Gets the specified task.
		 * @param taskId The identifier for the task to retrieve.
		 * @return The <code>TaskDescription</code> having the specified
		 * 		<code>taskId</code>, or <code>null</code> if no such task is
		 * 		found.
		 */
		public synchronized TaskDescription getTask(int taskId) {
			return tasks.get(taskId);
		}

		/**
		 * Determines whether the specified task exists.
		 * @param taskId The identifier for the task to look up.
		 * @return A value indicating whether a task exists with the given
		 * 		<code>taskId</code>.
		 */
		public synchronized boolean contains(int taskId) {
			return tasks.containsKey(taskId);
		}

		/**
		 * Obtains the next task to be served for this job.
		 * @return The <code>TaskDescription</code> for the next task to be
		 * 		served.
		 */
		public synchronized TaskDescription getNextTask() {
			if (taskQueue.isEmpty()) {
				return null;
			}
			int taskId = taskQueue.remove();
			taskQueue.addLast(taskId);
			return tasks.get(taskId);
		}

		/**
		 * Removes a task from the queue for this job.
		 * @param taskId The task ID of the task to be removed.
		 * @return The <code>Object</code> describing the removed task.
		 */
		public synchronized TaskDescription removeTask(int taskId) {
			taskQueue.remove((Object) new Integer(taskId));
			return tasks.remove(taskId);
		}

		/**
		 * Sets the priority for this job.
		 * @param priority The priority for this job.
		 */
		public void setPriority(int priority) {
			this.priority = priority;
		}

	}

	/**
	 * Compares two <code>UUID</code>s representing jobs according to their
	 * priority then according to the order in which they were first seen.
	 * @author Brad Kimmel
	 */
	private final class JobIdComparator implements Comparator<UUID> {

		/* (non-Javadoc)
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 */
		public int compare(UUID id1, UUID id2) {
			JobInfo job1 = jobs.get(id1);
			JobInfo job2 = jobs.get(id2);
			if (job1 == null || job2 == null) {
				throw new IllegalArgumentException("Either id1 or id2 represent a non-existant job.");
			}
			return job1.compareTo(job2);
		}

	}

	/**
	 * Gets the bookkeeping information for a job.  If the specified job has
	 * not been seen, a new <code>JobInfo</code> is created for it.
	 * @param jobId The <code>UUID</code> of the job for which to obtain the
	 * 		corresponding <code>JobInfo</code>.
	 * @return The <code>JobInfo</code> for the specified job.
	 */
	private JobInfo getJob(UUID jobId) {
		JobInfo job = jobs.get(jobId);
		if (job == null) {
			job = new JobInfo(jobId);
			jobs.put(jobId, job);
			jobQueue.add(jobId);
		}
		return job;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.server.scheduling.TaskScheduler#add(ca.eandb.jdcp.job.TaskDescription)
	 */
	public void add(TaskDescription task) {
		UUID jobId = task.getJobId();
		JobInfo job = getJob(jobId);
		if (!jobQueue.contains(jobId)) {
			jobQueue.add(jobId);
		}
		job.addTask(task);
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.server.scheduling.TaskScheduler#get(java.util.UUID, int)
	 */
	public TaskDescription get(UUID jobId, int taskId) {
		JobInfo job = getJob(jobId);
		return (job != null) ? job.getTask(taskId) : null;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.server.scheduling.TaskScheduler#contains(java.util.UUID, int)
	 */
	public boolean contains(UUID jobId, int taskId) {
		JobInfo job = getJob(jobId);
		return (job != null) ? job.contains(taskId) : false;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.scheduling.TaskScheduler#getNextTask()
	 */
	public TaskDescription getNextTask() {
		TaskDescription desc = null;

		while (true) {
			UUID jobId = jobQueue.peek();
			if (jobId == null) {
				break;
			}

			JobInfo job = getJob(jobId);
			desc = job.getNextTask();
			if (desc == null) {
				jobQueue.remove();
			} else {
				break;
			}
		}

		return desc;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.scheduling.TaskScheduler#remove(java.util.UUID, int)
	 */
	public TaskDescription remove(UUID jobId, int taskId) {
		JobInfo job = jobs.get(jobId);
		return (job != null) ? job.removeTask(taskId) : null;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.scheduling.TaskScheduler#setJobPriority(java.util.UUID, int)
	 */
	public void setJobPriority(UUID jobId, int priority) {
		JobInfo job = jobs.get(jobId);
		jobQueue.remove(jobId);
		job.setPriority(priority);
		jobQueue.add(jobId);
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.scheduling.TaskScheduler#removeJob(java.util.UUID)
	 */
	public void removeJob(UUID jobId) {
		jobQueue.remove(jobId);
		jobs.remove(jobId);
	}

}
