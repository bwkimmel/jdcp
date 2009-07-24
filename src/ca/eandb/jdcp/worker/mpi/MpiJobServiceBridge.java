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
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.security.auth.login.LoginException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import ca.eandb.jdcp.JdcpUtil;
import ca.eandb.jdcp.remote.AuthenticationService;
import ca.eandb.jdcp.remote.JobService;
import ca.eandb.jdcp.worker.JobServiceFactory;
import ca.eandb.jdcp.worker.ReconnectingJobService;
import ca.eandb.util.UnexpectedException;
import ca.eandb.util.rmi.Serialized;

/**
 * @author Brad
 *
 */
public final class MpiJobServiceBridge {

	static {
		System.out.println("Initializing");
		JdcpUtil.initialize();
		System.out.println("Setting up properties");
		PropertyConfigurator.configure(System.getProperties());

		JobServiceFactory serviceFactory = new JobServiceFactory() {
			public JobService connect() {
				return waitForService("kirk.home.eandb.ca", "brad-w", "tHH3hJL0", 10);
			}
		};

		Logger logger = Logger.getLogger(MpiJobServiceBridge.class);
		logger.info("Initializing job service");
		setJobService(new ReconnectingJobService(serviceFactory));
		logger.info("Job service initialized");

	}

	private static final Logger logger = Logger.getLogger(MpiJobServiceBridge.class);

	private static final int GET_CLASS_DEFINITION = 1;
	private static final int GET_CLASS_DIGEST = 2;
	private static final int GET_FINISHED_TASKS = 3;
	private static final int GET_TASK_WORKER = 4;
	private static final int REPORT_EXCEPTION = 5;
	private static final int REQUEST_TASK = 6;
	private static final int SUBMIT_TASK_RESULTS = 7;

	private static JobService service;

	private static Map<UUID, byte[]> taskWorkerCache = new HashMap<UUID, byte[]>();

	private static Map<String, byte[]> classDigestCache = new HashMap<String, byte[]>();

	private static Map<String, byte[]> classDefinitionCache = new HashMap<String, byte[]>();

	private static JobService waitForService(String host, String username, String password, int retryInterval) {
		JobService service = null;
		while (true) {
			service = connect(host, username, password);
			if (service != null) {
				break;
			}

			logger.info("CONNECTION FAILED");
			for (int i = retryInterval; i > 0; i--) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					return null;
				}
			}
		}
		return service;
	}

	private static JobService connect(String host, String username, String password) {
		JobService service = null;
		try {
			Registry registry = LocateRegistry.getRegistry(host, 5327);
			AuthenticationService auth = (AuthenticationService) registry.lookup("AuthenticationService");
			service = auth.authenticate(username, password);
		} catch (NotBoundException e) {
			logger.error("Job service not found at remote host.", e);
		} catch (RemoteException e) {
			logger.error("Could not connect to job service.", e);
		} catch (LoginException e) {
			logger.error("Login failed.", e);
		}
		return service;
	}

	public synchronized static void setJobService(JobService service) {
		MpiJobServiceBridge.service = service;
	}

	public static void call(int methodId, int caller, byte[] payload) {
		new MethodHandler(methodId, caller, payload).run();
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
			String name;
			UUID jobId = null;
			String key = null;
			byte[] payload = null;

			try {
				ByteArrayInputStream bis = new ByteArrayInputStream(params);
				ObjectInputStream ois = new ObjectInputStream(bis);

				switch (methodId) {
				case GET_CLASS_DEFINITION:
					name = (String) ois.readObject();
					jobId = (UUID) ois.readObject();
					if (logger.isInfoEnabled()) {
						logger.info(String.format("GET_CLASS_DEFINITION: jobId=%s, name=%s", jobId.toString(), name));
					}
					key = jobId.toString() + name;
					payload = classDefinitionCache.get(key);
					if (payload == null) {
						retVal = service.getClassDefinition(name, jobId);
					}
					break;

				case GET_CLASS_DIGEST:
					name = (String) ois.readObject();
					jobId = (UUID) ois.readObject();
					if (logger.isInfoEnabled()) {
						logger.info(String.format("GET_CLASS_DEFINITION: jobId=%s, name=%s", jobId.toString(), name));
					}
					key = jobId.toString() + name;
					payload = classDefinitionCache.get(key);
					if (payload == null) {
						retVal = service.getClassDigest(name, jobId);
					}
					break;

				case GET_FINISHED_TASKS:
					logger.info("GET_FINISHED_TASKS");
					retVal = service
							.getFinishedTasks((UUID[]) ois.readObject(),
									(int[]) ois.readObject());
					break;

				case GET_TASK_WORKER:
					logger.info("GET_TASK_WORKER");
					jobId = (UUID) ois.readObject();
					payload = taskWorkerCache.get(jobId);
					if (payload == null) {
						retVal = service.getTaskWorker(jobId);
					}
					break;

				case REPORT_EXCEPTION:
					logger.info("REPORT_EXCEPTION");
					service.reportException((UUID) ois.readObject(),
							(Integer) ois.readObject(), (Exception) ois
									.readObject());
					retVal = new byte[0];
					break;

				case REQUEST_TASK:
					logger.info("REQUEST_TASK");
					retVal = service.requestTask();
					break;

				case SUBMIT_TASK_RESULTS:
					logger.info("SUBMIT_TASK_RESULTS");
					service.submitTaskResults((UUID) ois.readObject(),
							(Integer) ois.readObject(),
							(Serialized<Object>) ois.readObject());
					retVal = new byte[0];
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
				if (payload == null) {
					ByteArrayOutputStream bos = new ByteArrayOutputStream();
					ObjectOutputStream oos = new ObjectOutputStream(bos);
					oos.writeBoolean(success);
					oos.writeObject(retVal);
					oos.flush();
					payload = bos.toByteArray();
					switch (methodId) {
					case GET_CLASS_DIGEST:
						classDigestCache.put(key, payload);
						break;
					case GET_CLASS_DEFINITION:
						classDefinitionCache.put(key, payload);
						break;
					case GET_TASK_WORKER:
						taskWorkerCache.put(jobId, payload);
						break;
					}
				}
				mpiReturn(caller, payload);
			} catch (IOException e) {
				logger.error("Could not serialized return value", e);
				throw new UnexpectedException(e);
			}

		}

	}

}
