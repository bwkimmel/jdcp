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

import ca.eandb.util.args.ArgumentProcessor;
import ca.eandb.util.args.BooleanFieldOption;
import ca.eandb.util.args.StringFieldOption;
import ca.eandb.util.args.UnrecognizedCommand;

/**
 * The main class for the JDCP client application.  This application
 * communicates with the JDCP server to manipulate it and to submit jobs to the
 * server.
 * @author Brad Kimmel
 */
public final class ClientMain {

	/**
	 * Application entry point.
	 * @param args Command line arguments.
	 */
	public static void main(String[] args) {

		ArgumentProcessor<Configuration> argProcessor = new ArgumentProcessor<Configuration>(true);

		argProcessor.addOption("verbose", 'V', new BooleanFieldOption<Configuration>("verbose"));
		argProcessor.addOption("host", 'h', new StringFieldOption<Configuration>("host"));
		argProcessor.addOption("username", 'u', new StringFieldOption<Configuration>("username"));
		argProcessor.addOption("password", 'p', new StringFieldOption<Configuration>("password"));

		argProcessor.addCommand("verify", new VerifyCommand());
		argProcessor.addCommand("sync", new SynchronizeCommand());
		argProcessor.addCommand("idle", new SetIdleTimeCommand());
		argProcessor.addCommand("script", new ScriptCommand());

		argProcessor.setDefaultCommand(UnrecognizedCommand.getInstance());

		argProcessor.process(args, new Configuration());

	}

}
