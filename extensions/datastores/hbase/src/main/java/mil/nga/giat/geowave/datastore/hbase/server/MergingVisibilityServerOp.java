package mil.nga.giat.geowave.datastore.hbase.server;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.server.RowMergingAdapterOptionProvider;

public class MergingVisibilityServerOp extends
		MergingServerOp
{

	@Override
	protected Mergeable getMergeable(
			final Cell cell,
			final byte[] bytes ) {
		return PersistenceUtils.fromBinary(
				bytes,
				Mergeable.class);
	}

	@Override
	protected byte[] getBinary(
			final Mergeable mergeable ) {
		return PersistenceUtils.toBinary(
				mergeable);
	}

	@Override
	public boolean nextRow(
			final List<Cell> rowCells )
			throws IOException {
		if (rowCells.size() > 1) {
			mergeList(
					rowCells);
		}
		return true;
	}

	@Override
	public void init(
			final Map<String, String> options )
			throws IOException {
		final String adapterIdsStr = options.get(
				RowMergingAdapterOptionProvider.ADAPTER_IDS_OPTION);

		if (adapterIdsStr.length() == 0) {
			throw new IllegalArgumentException(
					"The " + RowMergingAdapterOptionProvider.ADAPTER_IDS_OPTION + " must not be empty");
		}
		adapterIds = Sets.newHashSet(
				Iterables.transform(
						Splitter.on(
								",").split(
										adapterIdsStr),
						new Function<String, ByteArrayId>() {

							@Override
							public ByteArrayId apply(
									final String input ) {
								return new ByteArrayId(
										input);
							}
						}));
	}
}