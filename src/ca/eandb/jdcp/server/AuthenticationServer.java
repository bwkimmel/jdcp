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

package ca.eandb.jdcp.server;

import java.rmi.RemoteException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.UnicastRemoteObject;

import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;


import ca.eandb.jdcp.remote.AuthenticationService;
import ca.eandb.jdcp.remote.JobService;
import ca.eandb.util.auth.FixedCallbackHandler;

/**
 * @author brad
 *
 */
public final class AuthenticationServer extends UnicastRemoteObject implements
		AuthenticationService {

	/**
	 *
	 */
	private static final long serialVersionUID = 6823054390091081114L;

	private static final String LOGIN_CONFIGURATION_NAME = "JobService";

	private final JobService service;

	/**
	 * @throws RemoteException
	 */
	public AuthenticationServer(JobService service) throws RemoteException {
		this.service = service;
	}

	/**
	 * @param port
	 * @throws RemoteException
	 */
	public AuthenticationServer(JobService service, int port) throws RemoteException {
		super(port);
		this.service = service;
	}

	/**
	 * @param port
	 * @param csf
	 * @param ssf
	 * @throws RemoteException
	 */
	public AuthenticationServer(JobService service, int port, RMIClientSocketFactory csf,
			RMIServerSocketFactory ssf) throws RemoteException {
		super(port, csf, ssf);
		this.service = service;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.AuthenticationService#authenticate(java.lang.String, java.lang.String)
	 */
	@Override
	public JobService authenticate(final String username, final String password)
			throws RemoteException, LoginException {

		CallbackHandler handler = FixedCallbackHandler.forNameAndPassword(username, password);
		LoginContext context = new LoginContext(LOGIN_CONFIGURATION_NAME, handler);
		context.login();

		return new JobServiceProxy(context.getSubject(), service);

	}

}
