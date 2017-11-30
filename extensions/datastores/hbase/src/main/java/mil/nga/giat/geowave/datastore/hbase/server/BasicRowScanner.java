package mil.nga.giat.geowave.datastore.hbase.server;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.Cell;

public class BasicRowScanner implements
		RowScanner
{

	private final List<Cell> list;

	public BasicRowScanner(
			final List<Cell> list ) {
		this.list = list;
	}

	@Override
	public boolean isMidRow() {
		return false;
	}

	@Override
	public List<Cell> nextCellsInRow() {
		return Collections.EMPTY_LIST;
	}

	@Override
	public boolean isDone() {
		return false;
	}

	@Override
	public List<Cell> currentCellsInRow() {
		return list;
	}

}
