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
import ca.eandb.jdcp.server.ServerUtil;
import ca.eandb.util.rmi.Serialized;

/**
 * Convenience methods for working with JDCP.
 * @author Brad Kimmel
 */
public final class JdcpUtil {

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
	 */
	public static UUID submitJob(ParallelizableJob job, String description,
			String host, String username, String password)
			throws SecurityException, RemoteException, ClassNotFoundException,
			JobExecutionException, LoginException, NotBoundException {

		Serialized<ParallelizableJob> payload = new Serialized<ParallelizableJob>(job);
		Registry registry = LocateRegistry.getRegistry(host, 5327);
		AuthenticationService auth = (AuthenticationService) registry.lookup("AuthenticationService");
		JobService service = auth.authenticate(username, password);

		return service.submitJob(payload, description);

	}

	/**
	 * Gets the currently active <code>HostService</code>.
	 * @return The active <code>HostService</code> if the current thread is
	 * 		executing on a job server, <code>null</code> otherwise.
	 */
	public static HostService getHostService() {
		return ServerUtil.getHostService();
	}

	/** This constructor is private to prevent instances from being created. */
	private JdcpUtil() {}

}
