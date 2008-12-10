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

import ca.eandb.jdcp.remote.AuthenticationService;
import ca.eandb.jdcp.remote.JobService;

/**
 * @author Brad Kimmel
 *
 */
public final class Configuration {

	public boolean verbose = false;
	public String host = "localhost";
	public String digestAlgorithm = "MD5";

	public String username = "guest";
	public String password = "";


	private JobService service = null;

	public JobService getJobService() {
		if (service == null) {
			try {
				Registry registry = LocateRegistry.getRegistry(host);
				AuthenticationService auth = (AuthenticationService) registry.lookup("AuthenticationService");
				service = auth.authenticate(username, password);
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
			}
		}
		return service;
	}

}
