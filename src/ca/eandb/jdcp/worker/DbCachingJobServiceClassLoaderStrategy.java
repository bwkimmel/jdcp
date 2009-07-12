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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.UUID;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import ca.eandb.jdcp.remote.JobService;
import ca.eandb.util.StringUtil;
import ca.eandb.util.sql.DbUtil;

/**
 * A <code>CachingJobServiceClassLoaderStrategy</code> that stores class
 * definitions in a database.
 * @author Brad Kimmel
 */
public final class DbCachingJobServiceClassLoaderStrategy extends
		CachingJobServiceClassLoaderStrategy {

	/** The <code>Logger</code> for this class. */
	private static final Logger logger = Logger.getLogger(DbCachingJobServiceClassLoaderStrategy.class);

	/** The <code>DataSource</code> to use to cache class definitions. */
	private final DataSource ds;

	/**
	 * Prepares the data source to store cached class definitions.
	 * @param ds The <code>DataSource</code> to prepare.
	 * @throws SQLException If an error occurs while communicating with the
	 * 		database.
	 */
	public static void prepareDataSource(DataSource ds) throws SQLException {
		Connection con = null;
		String sql;
		try {
			con = ds.getConnection();
			con.setAutoCommit(false);

			DatabaseMetaData meta = con.getMetaData();
			ResultSet rs = meta.getTables(null, null, null, new String[]{"TABLE"});
			int tableNameColumn = rs.findColumn("TABLE_NAME");
			int count = 0;
			while (rs.next()) {
				String tableName = rs.getString(tableNameColumn);
				if (tableName.equalsIgnoreCase("CachedClasses")) {
					count++;
				}
			}

			if (count == 0) {
				String blobType = DbUtil.getTypeName(Types.BLOB, con);
				String nameType = DbUtil.getTypeName(Types.VARCHAR, 1024, con);
				String md5Type = DbUtil.getTypeName(Types.BINARY, 16, con);

				sql =	"CREATE TABLE CachedClasses ( \n" +
						"  Name " + nameType + " NOT NULL, \n" +
						"  MD5 " + md5Type + " NOT NULL, \n" +
						"  Definition " + blobType + " NOT NULL, \n" +
						"  PRIMARY KEY (Name, MD5) \n" +
						")";
				DbUtil.update(ds, sql);
				con.commit();
			}

			con.setAutoCommit(true);
		} catch (SQLException e) {
			DbUtil.rollback(con);
			throw e;
		} finally {
			DbUtil.close(con);
		}
	}

	/**
	 * Creates a new <code>DbCachingJobServiceClassLoaderStrategy</code>.
	 * @param service The <code>JobService</code> from which to obtain class
	 * 		definitions.
	 * @param jobId The <code>UUID</code> identifying the job for which to
	 * 		obtain class definitions.
	 * @param ds The <code>DataSource</code> to use to store cached class
	 * 		definitions.
	 */
	public DbCachingJobServiceClassLoaderStrategy(JobService service,
			UUID jobId, DataSource ds) {
		super(service, jobId);
		this.ds = ds;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.worker.CachingJobServiceClassLoaderStrategy#cacheLookup(java.lang.String, byte[])
	 */
	@Override
	protected byte[] cacheLookup(String name, byte[] digest) {
		String sql =
				"SELECT Definition " +
				"FROM CachedClasses " +
				"WHERE Name = ? " +
				"  AND MD5 = ?";

		try {
			return DbUtil.queryBinary(ds, null, sql, name, digest);
		} catch (SQLException e) {
			logger.error("Database error", e);
		}

		return null;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.worker.CachingJobServiceClassLoaderStrategy#cacheStore(java.lang.String, byte[], byte[])
	 */
	@Override
	protected void cacheStore(String name, byte[] digest, byte[] def) {
		try {
			String sql =
					"SELECT COUNT(1) " +
					"FROM CachedClasses " +
					"WHERE Name = ? " +
					"  AND MD5 = ?";
			if (DbUtil.queryInt(ds, 0, sql, name, digest) > 0) {
				String message = String.format("Overwriting class definition: name='%s', digest=%s", name, StringUtil.toHex(digest));
				logger.warn(message);
				DbUtil.update(ds,
						"UPDATE CachedClasses " +
						"SET Definition = ? " +
						"WHERE Name = ? " +
						"  AND MD5 = ?",
						def, name, digest);
			} else {
				DbUtil.update(ds,
						"INSERT INTO CachedClasses " +
						"  (Name, MD5, Definition) " +
						"VALUES " +
						"  (?, ?, ?)",
						name, digest, def);
			}
		} catch (SQLException e) {
			logger.error("Database error", e);
		}
	}

}
