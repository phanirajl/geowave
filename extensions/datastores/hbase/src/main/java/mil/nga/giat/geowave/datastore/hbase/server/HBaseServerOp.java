package mil.nga.giat.geowave.datastore.hbase.server;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;

public interface HBaseServerOp
{
	/**
	 *
	 * @param rowCells
	 *            the cells of the current row
	 * @return true to continue iteration - false will end the scan, resulting
	 *         in no more subsequent rows (most situations should be true)
	 *
	 * @throws IOException
	 *             e if an exception occurs during iteration
	 */
	public boolean nextRow(
			List<Cell> rowCells )
			throws IOException;

	public void init(
			Map<String, String> options )
			throws IOException;
}
