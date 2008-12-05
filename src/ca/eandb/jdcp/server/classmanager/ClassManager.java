/**
 *
 */
package ca.eandb.jdcp.server.classmanager;

import java.nio.ByteBuffer;

import ca.eandb.util.classloader.ClassLoaderStrategy;

/**
 * @author brad
 *
 */
public interface ClassManager extends ClassLoaderStrategy {

	void setClassDefinition(String name, ByteBuffer def);

	void setClassDefinition(String name, byte[] def);

	byte[] getClassDigest(String name);

}
