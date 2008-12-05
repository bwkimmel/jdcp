/**
 *
 */
package ca.eandb.jdcp.client;

import ca.eandb.util.args.ArgumentProcessor;
import ca.eandb.util.args.BooleanFieldOption;
import ca.eandb.util.args.StringFieldOption;
import ca.eandb.util.args.UnrecognizedCommand;

/**
 * @author brad
 *
 */
public final class ClientMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		ArgumentProcessor<Configuration> argProcessor = new ArgumentProcessor<Configuration>();

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
