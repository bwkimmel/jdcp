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

package ca.eandb.jdcp.worker.mpi;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import ca.eandb.jdcp.remote.JobService;
import ca.eandb.util.UnexpectedException;
import ca.eandb.util.concurrent.BackgroundThreadFactory;
import ca.eandb.util.rmi.Serialized;

/**
 * @author Brad
 *
 */
public final class MpiJobServiceBridge {

	private static final Logger logger = Logger.getLogger(MpiJobServiceBridge.class);

	private static final int GET_CLASS_DEFINITION = 1;
	private static final int GET_CLASS_DIGEST = 2;
	private static final int GET_FINISHED_TASKS = 3;
	private static final int GET_TASK_WORKER = 4;
	private static final int REPORT_EXCEPTION = 5;
	private static final int REQUEST_TASK = 6;
	private static final int SUBMIT_TASK_RESULTS = 7;

	private static JobService service;

	private static Executor executor = Executors.newCachedThreadPool(new BackgroundThreadFactory());

	public synchronized static void setJobService(JobService service) {
		MpiJobServiceBridge.service = service;
	}

	public static void call(int methodId, int caller, byte[] payload) {
		executor.execute(new MethodHandler(methodId, caller, payload));
	}

	private static native void mpiReturn(int caller, byte[] payload);

	private static class MethodHandler implements Runnable {

		private final int methodId;
		private final int caller;
		private final byte[] params;

		public MethodHandler(int methodId, int caller, byte[] params) {
			this.methodId = methodId;
			this.caller = caller;
			this.params = params;
		}

		@SuppressWarnings("unchecked")
		public void run() {

			Object retVal = null;
			boolean success = true;

			try {
				ByteArrayInputStream bis = new ByteArrayInputStream(params);
				ObjectInputStream ois = new ObjectInputStream(bis);

				switch (methodId) {
				case GET_CLASS_DEFINITION:
					retVal = service.getClassDefinition((String) ois
							.readObject(), (UUID) ois.readObject());
					break;

				case GET_CLASS_DIGEST:
					retVal = service.getClassDigest((String) ois.readObject(),
							(UUID) ois.readObject());
					break;

				case GET_FINISHED_TASKS:
					retVal = service
							.getFinishedTasks((UUID[]) ois.readObject(),
									(int[]) ois.readObject());
					break;

				case GET_TASK_WORKER:
					retVal = service.getTaskWorker((UUID) ois.readObject());
					break;

				case REPORT_EXCEPTION:
					service.reportException((UUID) ois.readObject(),
							(Integer) ois.readObject(), (Exception) ois
									.readObject());
					retVal = new Object();
					break;

				case REQUEST_TASK:
					retVal = service.requestTask();
					break;

				case SUBMIT_TASK_RESULTS:
					service.submitTaskResults((UUID) ois.readObject(),
							(Integer) ois.readObject(),
							(Serialized<Object>) ois.readObject());
					retVal = new Object();
					break;

				default:
					throw new IllegalArgumentException("methodId");
				}
			} catch (IOException e) {
				logger.error("Could not extract method parameters.", e);
				throw new UnexpectedException(e);
			} catch (Exception e) {
				success = false;
				retVal = e;
			}

			try {
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(bos);
				oos.writeBoolean(success);
				oos.writeObject(retVal);
				oos.flush();
				mpiReturn(caller, bos.toByteArray());
			} catch (IOException e) {
				logger.error("Could not serialized return value", e);
				throw new UnexpectedException(e);
			}

		}

	}

}
