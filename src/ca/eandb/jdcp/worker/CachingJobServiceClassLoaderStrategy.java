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

package ca.eandb.jdcp.worker;

import java.nio.ByteBuffer;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;


import ca.eandb.jdcp.remote.JobService;
import ca.eandb.util.classloader.ClassLoaderStrategy;

/**
 * An abstract <code>ClassLoaderStrategy</code> that obtains class defintions
 * from a <code>JobService</code>.  Class definitions are cached and only
 * obtained from the <code>JobService</code> if the MD5 hash does not match a
 * definition of that class in the cache (multiple definitions of the same
 * class may be cached).
 * @author Brad Kimmel
 */
public abstract class CachingJobServiceClassLoaderStrategy implements ClassLoaderStrategy {

	/** The <code>Logger</code> for this class. */
	private static final Logger logger = Logger.getLogger(CachingJobServiceClassLoaderStrategy.class);

	/** The <code>JobService</code> from which to obtain class definitions. */
	private final JobService service;

	/**
	 * The <code>UUID</code> identifying the job for which to obtain class
	 * definitions.
	 */
	private final UUID jobId;

	/**
	 * A <code>Map</code> that stores the digests associated with each class.
	 */
	private Map<String, byte[]> digestLookup = new HashMap<String, byte[]>();

	/**
	 * Creates a new <code>CachingJobServiceClassLoaderStrategy</code>.
	 * @param service The <code>JobService</code> from which to obtain class
	 * 		definitions.
	 * @param jobId The <code>UUID</code> identifying the job for which to
	 * 		obtain class definitions.
	 */
	protected CachingJobServiceClassLoaderStrategy(JobService service, UUID jobId) {
		this.service = service;
		this.jobId = jobId;
	}

	/**
	 * Gets the digest associated with a given class.
	 * @param name The name of the class.
	 * @return The class digest.
	 */
	public synchronized final byte[] getClassDigest(String name) {
		byte[] digest = digestLookup.get(name);
		if (digest == null) {
			try {
				digest = service.getClassDigest(name, jobId);
				digestLookup.put(name, digest);
			} catch (SecurityException e) {
				logger.error("Could not get class digest", e);
			} catch (RemoteException e) {
				logger.error("Could not get class digest", e);
			}
		}
		return digest;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.util.classloader.ClassLoaderStrategy#getClassDefinition(java.lang.String)
	 */
	public final ByteBuffer getClassDefinition(String name) {

		try {

			byte[] digest = getClassDigest(name);
			byte[] def = cacheLookup(name, digest);

			if (def == null) {
				def = service.getClassDefinition(name, jobId);
				if (def != null) {
					cacheStore(name, digest, def);
				}
			}

			if (def != null) {
				return ByteBuffer.wrap(def);
			}

		} catch (RemoteException e) {
			logger.error("Could not get class definition", e);
		}

		return null;

	}

	/**
	 * Looks up a class definition in the cache.
	 * @param name The fully qualified name of the class to look up.
	 * @param digest The MD5 digest of the class definition.
	 * @return The matching class definition, or null if no definition of
	 * 		the specified class having the specified MD5 digest exists in
	 * 		the cache.
	 */
	protected abstract byte[] cacheLookup(String name, byte[] digest);

	/**
	 * Stores a class definition in the cache.
	 * @param name The fully qualified name of the class to store.
	 * @param digest The MD5 digest of the class definition.
	 * @param def The class definition.
	 */
	protected abstract void cacheStore(String name, byte[] digest, byte[] def);

}
