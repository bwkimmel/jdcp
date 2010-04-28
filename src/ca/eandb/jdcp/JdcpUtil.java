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

package ca.eandb.jdcp;

import java.io.File;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.UUID;

import javax.security.auth.login.LoginException;

import ca.eandb.jdcp.job.HostService;
import ca.eandb.jdcp.job.JobExecutionException;
import ca.eandb.jdcp.job.ParallelizableJob;
import ca.eandb.jdcp.remote.AuthenticationService;
import ca.eandb.jdcp.remote.JobService;
import ca.eandb.jdcp.remote.ProtocolVersionException;
import ca.eandb.jdcp.remote.TaskService;
import ca.eandb.jdcp.server.ServerUtil;
import ca.eandb.util.io.FileUtil;
import ca.eandb.util.rmi.Serialized;

/**
 * Convenience methods for working with JDCP.
 * @author Brad Kimmel
 */
public final class JdcpUtil {

	/** The default port that a JDCP server listens on. */
	public static final int DEFAULT_PORT = 5327;

	/**
	 * Uniquely identifies the protocol used for communication between clients
	 * and a server.  This is used for verifying protocol compatibility when
	 * authenticating with the server.
	 */
	public static final UUID PROTOCOL_VERSION_ID = UUID.fromString("F41F779C-0208-4BAC-AE5B-6E3FFD3FB903");

	/**
	 * Submits a job to a server for processing.
	 * @param job The <code>ParallelizableJob</code> to be processed.
	 * @param description A description of the job.
	 * @param host The host name of the server to send the job to.
	 * @param username The user name to use to authenticate with the server.
	 * @param password The password to use to authenticate with the server.
	 * @return The <code>UUID</code> assigned to the job.
	 * @throws SecurityException If the user does not have access to perform
	 * 		the requested action on the server.
	 * @throws RemoteException If a failure occurs in attempting to communicate
	 * 		with the server.
	 * @throws ClassNotFoundException If deserialization of the job at the
	 * 		server requires a class that could not be found on the server.
	 * @throws JobExecutionException If the submitted job threw an exception at
	 * 		the server during initialization.
	 * @throws LoginException If the login attempt fails.
	 * @throws NotBoundException If the <code>AuthenticationService</code>
	 * 		could not be found at the server.
	 * @throws ProtocolVersionException If this client is incompatible with the
	 * 		server.
	 */
	public static UUID submitJob(ParallelizableJob job, String description,
			String host, String username, String password)
			throws SecurityException, RemoteException, ClassNotFoundException,
			JobExecutionException, LoginException, NotBoundException, ProtocolVersionException {

		Serialized<ParallelizableJob> payload = new Serialized<ParallelizableJob>(job);
		Registry registry = LocateRegistry.getRegistry(host, DEFAULT_PORT);
		AuthenticationService auth = (AuthenticationService) registry.lookup("AuthenticationService");
		JobService service = auth.authenticate(username, password, PROTOCOL_VERSION_ID);

		return service.submitJob(payload, description);

	}

	/**
	 * Submits a job to a server for processing.
	 * @param job The <code>ParallelizableJob</code> to be processed.
	 * @param description A description of the job.
	 * @param host The host name of the server to send the job to.
	 * @param username The user name to use to authenticate with the server.
	 * @param password The password to use to authenticate with the server.
	 * @return The <code>UUID</code> assigned to the job.
	 * @throws SecurityException If the user does not have access to perform
	 * 		the requested action on the server.
	 * @throws RemoteException If a failure occurs in attempting to communicate
	 * 		with the server.
	 * @throws ClassNotFoundException If deserialization of the job at the
	 * 		server requires a class that could not be found on the server.
	 * @throws JobExecutionException If the submitted job threw an exception at
	 * 		the server during initialization.
	 * @throws LoginException If the login attempt fails.
	 * @throws NotBoundException If the <code>AuthenticationService</code>
	 * 		could not be found at the server.
	 * @throws ProtocolVersionException If this client is incompatible with the
	 * 		server.
	 */
	public static void registerTaskService(String name, TaskService taskService,
			String host, String username, String password)
			throws SecurityException, RemoteException, ClassNotFoundException,
			JobExecutionException, LoginException, NotBoundException, ProtocolVersionException {

		Registry registry = LocateRegistry.getRegistry(host, DEFAULT_PORT);
		AuthenticationService auth = (AuthenticationService) registry.lookup("AuthenticationService");
		JobService service = auth.authenticate(username, password, PROTOCOL_VERSION_ID);

//		TaskService stub = (TaskService) UnicastRemoteObject.exportObject(taskService, JdcpUtil.DEFAULT_PORT+1);

		service.registerTaskService(name, taskService);

	}

	/**
	 * Gets the currently active <code>HostService</code>.
	 * @return The active <code>HostService</code> if the current thread is
	 * 		executing on a job server, <code>null</code> otherwise.
	 */
	public static HostService getHostService() {
		return ServerUtil.getHostService();
	}

	/**
	 * Gets the folder at which JDCP application data is stored.
	 * @return The <code>File</code> representing the folder at which JDCP
	 * 		application data is stored.
	 */
	public static File getHomeDirectory() {
		initialize();
		return new File(System.getProperty("jdcp.home"));
	}

	/**
	 * Performs initialization for the currently running JDCP
	 * application.
	 */
	public static void initialize() {
		String homeDirName = System.getProperty("jdcp.home");
		File homeDir;
		if (homeDirName != null) {
			homeDir = new File(homeDirName);
		} else {
			homeDir = FileUtil.getApplicationDataDirectory("jdcp");
			System.setProperty("jdcp.home", homeDir.getPath());
		}
		homeDir.mkdir();
		if (System.getProperty("derby.system.home") == null) {
			System.setProperty("derby.system.home", homeDir.getPath());
		}
	}

	/** This constructor is private to prevent instances from being created. */
	private JdcpUtil() {}

}
