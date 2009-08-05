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

package ca.eandb.jdcp.client;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import javax.security.auth.login.LoginException;

import ca.eandb.jdcp.JdcpUtil;
import ca.eandb.jdcp.remote.AuthenticationService;
import ca.eandb.jdcp.remote.JobService;
import ca.eandb.jdcp.remote.ProtocolVersionException;

/**
 * Command line options for the JDCP client application.
 * @author Brad Kimmel
 */
public class Configuration {

	/**
	 * A value indicating whether the application should display additional
	 * diagnostic information.
	 */
	public boolean verbose = false;

	/**
	 * The name or IP address of the JDCP server host.
	 */
	public String host = "localhost";

	/**
	 * The username to use for authentication with the JDCP server.
	 */
	public String username = "guest";

	/**
	 * The password to use for authentication with the JDCP server.
	 */
	public String password = "";

	/**
	 * The digest algorithm that the server uses when it is queried for a hash
	 * of a particular class.
	 */
	public String digestAlgorithm = "MD5";

	/**
	 * The <code>JobService</code> that the application is connected to.
	 */
	private JobService service = null;

	/**
	 * Obtains the <code>JobService</code> to use for this client session.
	 * @return
	 */
	public JobService getJobService() {
		if (service == null) {
			try {
				Registry registry = LocateRegistry.getRegistry(host, JdcpUtil.DEFAULT_PORT);
				AuthenticationService auth = (AuthenticationService) registry.lookup("AuthenticationService");
				service = auth.authenticate(username, password, JdcpUtil.PROTOCOL_VERSION_ID);
			} catch (NotBoundException e) {
				System.err.println("Job service not found at remote host.");
				System.exit(1);
			} catch (RemoteException e) {
				System.err.println("Could not connect to job service.");
				e.printStackTrace();
				System.exit(1);
			} catch (LoginException e) {
				System.err.println("Login failed.");
				System.exit(1);
			} catch (ProtocolVersionException e) {
				System.err.println("This client is incompatible with the remote server.  Please update the client and try again.");
				System.exit(1);
			}
		}
		return service;
	}

}
