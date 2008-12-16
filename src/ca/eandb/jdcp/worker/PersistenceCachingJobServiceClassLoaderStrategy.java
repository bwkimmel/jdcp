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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.UUID;

import javax.jnlp.BasicService;
import javax.jnlp.FileContents;
import javax.jnlp.PersistenceService;
import javax.jnlp.ServiceManager;
import javax.jnlp.UnavailableServiceException;


import ca.eandb.jdcp.remote.JobService;
import ca.eandb.util.StringUtil;
import ca.eandb.util.UnexpectedException;

/**
 * A <code>CachingJobServiceClassLoaderStrategy</code> that uses the JNLP
 * <code>PersistenceService</code> to store class definitions.
 * @author Brad Kimmel
 */
public final class PersistenceCachingJobServiceClassLoaderStrategy extends
		CachingJobServiceClassLoaderStrategy {

	/** The base <code>URL</code> to use for storing class definitions. */
	private final URL baseUrl;

	/**
	 * The <code>PersistenceService</code> to use for storing class
	 * definitions.
	 */
	private final PersistenceService persistenceService;

	/**
	 * Creates a new
	 * <code>PersistenceCachingJobServiceClassLoaderStrategy</code>.  The
	 * <code>BasicService</code> is used to determine the code base
	 * <code>URL</code>.
	 * @param service The <code>JobService</code> from which to obtain class
	 * 		definitions.
	 * @param jobId The <code>UUID</code> identifying the job for which to
	 * 		obtain class definitions.
	 * @throws UnavailableServiceException If <code>PersistenceService</code>
	 * 		or <code>BasicService</code> is unavailable.
	 * @see javax.jnlp.PersistenceService
	 * @see javax.jnlp.BasicService
	 */
	public PersistenceCachingJobServiceClassLoaderStrategy(JobService service,
			UUID jobId) throws UnavailableServiceException {
		super(service, jobId);
		BasicService basicService = (BasicService) ServiceManager.lookup("javax.jnlp.BasicService");
		this.baseUrl = basicService.getCodeBase();
		this.persistenceService = (PersistenceService) ServiceManager.lookup("javax.jnlp.PersistenceService");
	}

	/**
	 * Creates a new
	 * <code>PersistenceCachingJobServiceClassLoaderStrategy</code>.
	 * @param service The <code>JobService</code> from which to obtain class
	 * 		definitions.
	 * @param jobId The <code>UUID</code> identifying the job for which to
	 * 		obtain class definitions.
	 * @param baseUrl The base <code>URL</code> to use for storing class
	 * 		definitions.
	 * @throws UnavailableServiceException If <code>PersistenceService</code>
	 * 		is unavailable.
	 * @see javax.jnlp.PersistenceService
	 */
	public PersistenceCachingJobServiceClassLoaderStrategy(JobService service,
			UUID jobId, URL baseUrl) throws UnavailableServiceException {
		super(service, jobId);
		this.baseUrl = baseUrl;
		this.persistenceService = (PersistenceService) ServiceManager.lookup("javax.jnlp.PersistenceService");
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.worker.CachingJobServiceClassLoaderStrategy#cacheLookup(java.lang.String, byte[])
	 */
	@Override
	protected byte[] cacheLookup(String name, byte[] digest) {

		try {

			URL url = getUrlForCacheEntry(name, digest);
			FileContents contents = persistenceService.get(url);

			/* If we get here, the digest is okay, so read the data. */
			InputStream in = contents.getInputStream();
			byte[] def = new byte[(int) contents.getLength()];

			in.read(def);

			return def;

		} catch (FileNotFoundException e) {

			/*
			 * Nothing to do.  This just means there is no cached item with the
			 * specified key.
			 */

		} catch (MalformedURLException e) {

			/*
			 * This should not happen.  getUrlForCacheEntry should ensure that the
			 * URL is valid.
			 */
			throw new UnexpectedException(e);

		} catch (IOException e) {

			e.printStackTrace();

		}

		return null;

	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.worker.CachingJobServiceClassLoaderStrategy#cacheStore(java.lang.String, byte[], byte[])
	 */
	@Override
	protected void cacheStore(String name, byte[] digest, byte[] def) {
		URL url = getUrlForCacheEntry(name, digest);
		write(url, def);
	}

	/**
	 * Gets the <code>URL</code> for storing the specified class definition.
	 * @param name The fully qualified name of the class.
	 * @param digest The MD5 digest of the class definition.
	 * @return The <code>URL</code> to use.
	 */
	private URL getUrlForCacheEntry(String name, byte[] digest) {
		try {
			return new URL(baseUrl, name.replace('.', '/') + StringUtil.toHex(digest));
		} catch (MalformedURLException e) {
			throw new UnexpectedException(e);
		}
	}

	/**
	 * Writes data to persistent storage.
	 * @param url The <code>URL</code> to associate with the data.
	 * @param data The data to write.
	 */
	private void write(URL url, byte[] data) {

		try {

			if (persistenceService.create(url, data.length) < data.length) {
				persistenceService.delete(url);
				throw new RuntimeException("Could not allocate enough space in persistence store.");
			}

			FileContents contents = persistenceService.get(url);
			OutputStream out = contents.getOutputStream(true);

			out.write(data);

		} catch (IOException e) {

			e.printStackTrace();
			throw new RuntimeException("Could not write data to persistence store.", e);

		}

	}

}
