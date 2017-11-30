package mil.nga.giat.geowave.datastore.hbase.server;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;

public interface RowScanner
{
	public boolean isMidRow();

	public List<Cell> nextCellsInRow()
			throws IOException;
	
	public boolean isDone();
	
	public List<Cell> currentCellsInRow();
}
