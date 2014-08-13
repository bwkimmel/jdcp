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

package ca.eandb.jdcp.console;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import org.apache.derby.jdbc.EmbeddedDataSource;
import org.apache.log4j.Logger;

import ca.eandb.jdcp.JdcpUtil;
import ca.eandb.jdcp.hub.JobHub;
import ca.eandb.jdcp.server.AuthenticationServer;
import ca.eandb.util.args.CommandArgument;
import ca.eandb.util.args.OptionArgument;

/**
 * @author Brad Kimmel
 */
public final class HubState {

	/** The <code>Logger</code> to log messages to. */
	private static final Logger logger = Logger.getLogger(HubState.class);

	/** The RMI <code>Registry</code> to register the server with. */
	private Registry registry = null;

	/** The running <code>JobHub</code>. */
	private JobHub jobHub = null;

	/**
	 * Gets the RMI <code>Registry</code> to register the server with, creating
	 * it if necessary.
	 * @return The RMI <code>Registry</code> to register the server with.
	 * @throws RemoteException If an error occurs while attempting to create
	 * 		the <code>Registry</code>.
	 */
	public synchronized Registry getRegistry() throws RemoteException {
		if (registry == null) {
			registry = LocateRegistry.createRegistry(JdcpUtil.DEFAULT_PORT);
		}
		return registry;
	}

	/**
	 * Starts the hub.
	 */
	@CommandArgument
	public void start() {
		System.out.println("Starting hub");
		try {

			Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
			EmbeddedDataSource ds = new EmbeddedDataSource();
			ds.setConnectionAttributes("create=true");
			ds.setDatabaseName("classes");

			JobHub.prepareDataSource(ds);

			logger.info("Initializing service");
			jobHub = new JobHub(ds);
			AuthenticationServer authServer = new AuthenticationServer(jobHub, JdcpUtil.DEFAULT_PORT);

			logger.info("Binding service");
			Registry registry = getRegistry();
			registry.bind("AuthenticationService", authServer);

			logger.info("Hub ready");
			System.out.println("Hub started");

		} catch (Exception e) {
			System.err.println("Failed to start hub");
			logger.error("Failed to start hub", e);
		}
	}

	/**
	 * Stops the hub.
	 */
	@CommandArgument
	public void stop() {
		try {
			jobHub.shutdown();
			jobHub = null;
			Registry registry = getRegistry();
			registry.unbind("AuthenticationService");
			System.out.println("Hub stopped");
		} catch (Exception e) {
			logger.error("An error occurred while stopping the hub", e);
			System.err.println("Hub did not shut down cleanly, see log for details.");
		}
	}

	@CommandArgument
	public void connect(
			@OptionArgument("host") String host,
			@OptionArgument("username") String username,
			@OptionArgument("password") String password) {

		JobHub hub = jobHub;
		if (hub == null) {
			System.err.println("Hub not running.");
		}
		if (host.equals("")) {
			host = "localhost";
		}
		if (username.equals("")) {
			username = "guest";
		}

		System.out.printf("Connecting hub to %s\n", host);
		hub.connect(host, username, password);
	}

	@CommandArgument
	public void disconnect(@OptionArgument("host") String host) {
		JobHub hub = jobHub;
		if (hub == null) {
			System.err.println("Hub not running.");
		}
		if (host.equals("")) {
			host = "localhost";
		}

		System.out.printf("Disconnecting hub from %s\n", host);
		hub.disconnect(host);
	}

	/**
	 * Prints the status of the hub.
	 */
	@CommandArgument
	public void stat() {
		if (this.jobHub == null) {
			System.err.println("Hub not running");
			return;
		}
		System.out.println("Hub running");
	}

}
