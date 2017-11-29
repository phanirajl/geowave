package mil.nga.giat.geowave.datastore.hbase.server;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.ScannerContext.LimitScope;

public class ServerOpInternalScannerWrapper implements
		InternalScanner
{
	protected final Collection<HBaseServerOp> orderedServerOps;
	protected InternalScanner delegate;

	public ServerOpInternalScannerWrapper(
			final Collection<HBaseServerOp> orderedServerOps,
			final InternalScanner delegate ) {
		this.orderedServerOps = orderedServerOps;
		this.delegate = delegate;
	}

	protected boolean internalNextRow(
			final List<Cell> rowCells )
			throws IOException {
		for (final HBaseServerOp serverOp : orderedServerOps) {
			if (!serverOp.nextRow(
					rowCells)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean next(
			final List<Cell> rowCells )
			throws IOException {
		boolean retVal = delegate.next(
				rowCells);
		if (!internalNextRow(
				rowCells)) {
			return false;
		}
		return retVal;
	}

	@Override
	public boolean next(
			final List<Cell> rowCells,
			final ScannerContext scannerContext )
			throws IOException {
		boolean retVal = delegate.next(
				rowCells,
				scannerContext);
		if (scannerContext.checkAnyLimitReached(LimitScope.BETWEEN_CELLS)){
		if (!internalNextRow(
				rowCells)) {
			return false;
		}
		}
		return retVal;
	}

	@Override
	public void close()
			throws IOException {
		delegate.close();
	}

}
