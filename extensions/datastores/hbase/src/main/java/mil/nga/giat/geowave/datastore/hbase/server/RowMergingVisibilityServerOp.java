package mil.nga.giat.geowave.datastore.hbase.server;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;

public class RowMergingVisibilityServerOp extends
		RowMergingServerOp
{

	@Override
	public boolean nextRow(
			List<Cell> rowCells )
			throws IOException {
		if (rowCells.size() > 1) {
			mergeList(
					rowCells);
		}
		return true;
	}

}
