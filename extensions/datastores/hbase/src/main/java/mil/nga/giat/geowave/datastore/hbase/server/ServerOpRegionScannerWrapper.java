package mil.nga.giat.geowave.datastore.hbase.server;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;

public class ServerOpRegionScannerWrapper extends
		ServerOpInternalScannerWrapper implements
		RegionScanner
{

	public ServerOpRegionScannerWrapper(
			final Collection<HBaseServerOp> orderedServerOps,
			final RegionScanner delegate ) {
		super(
				orderedServerOps,
				delegate);
	}

	@Override
	public HRegionInfo getRegionInfo() {
		if (delegate != null) {
			return ((RegionScanner) delegate).getRegionInfo();
		}
		return null;
	}

	@Override
	public boolean isFilterDone()
			throws IOException {
		if (delegate != null) {
			return ((RegionScanner) delegate).isFilterDone();
		}
		return false;
	}

	@Override
	public boolean reseek(
			final byte[] row )
			throws IOException {
		if (delegate != null) {
			return ((RegionScanner) delegate).reseek(
					row);
		}
		return false;
	}

	@Override
	public long getMaxResultSize() {
		if (delegate != null) {
			return ((RegionScanner) delegate).getMaxResultSize();
		}
		return Long.MAX_VALUE;
	}

	@Override
	public long getMvccReadPoint() {
		if (delegate != null) {
			return ((RegionScanner) delegate).getMvccReadPoint();
		}
		return 0;
	}

	@Override
	public int getBatch() {
		if (delegate != null) {
			return ((RegionScanner) delegate).getBatch();
		}
		return -1;
	}

	@Override
	public boolean nextRaw(
			final List<Cell> rowCells )
			throws IOException {
		if (!internalNextRow(
				rowCells)) {
			return false;
		}
		if (delegate != null) {
			return ((RegionScanner) delegate).nextRaw(
					rowCells);
		}
		return true;
	}

	@Override
	public boolean nextRaw(
			final List<Cell> rowCells,
			final ScannerContext scannerContext )
			throws IOException {
		if (!internalNextRow(
				rowCells)) {
			return false;
		}
		if (delegate != null) {
			return ((RegionScanner) delegate).nextRaw(
					rowCells,
					scannerContext);
		}
		return true;
	}
}
