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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import ca.eandb.jdcp.remote.TaskService;
import ca.eandb.util.StringUtil;

/**
 * A <code>CachingJobServiceClassLoaderStrategy</code> that stores class
 * definitions internally.
 * @author Brad Kimmel
 */
public final class InternalCachingJobServiceClassLoaderStrategy extends
    CachingJobServiceClassLoaderStrategy {

  /** A <code>Map</code> storing cached class definitions. */
  private static final Map<String, byte[]> cache = new HashMap<String, byte[]>();

  /**
   * Creates a new <code>InternalCachingJobServiceClassLoaderStrategy</code>.
   * @param service The <code>TaskService</code> from which to obtain class
   *     definitions.
   * @param jobId The <code>UUID</code> identifying the job for which to
   *     obtain class definitions.
   */
  public InternalCachingJobServiceClassLoaderStrategy(TaskService service,
      UUID jobId) {
    super(service, jobId);
  }

  /**
   * Gets a unique key corresponding to the specified class name/digest pair.
   * @param name The name of the class.
   * @param digest The class digest.
   * @return The key to use for the cache map.
   */
  private String getKey(String name, byte[] digest) {
    return name + "$$" + StringUtil.toHex(digest);
  }

  @Override
  protected byte[] cacheLookup(String name, byte[] digest) {
    return cache.get(getKey(name, digest));
  }

  @Override
  protected void cacheStore(String name, byte[] digest, byte[] def) {
    cache.put(getKey(name, digest), def);
  }

}
