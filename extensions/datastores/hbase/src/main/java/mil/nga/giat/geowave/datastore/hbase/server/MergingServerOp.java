package mil.nga.giat.geowave.datastore.hbase.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Scan;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.server.RowMergingAdapterOptionProvider;

public class MergingServerOp implements
		HBaseServerOp
{
	protected Set<ByteArrayId> adapterIds = new HashSet<>();
	private static final String OLD_MAX_VERSIONS_KEY = "MAX_VERSIONS";

	protected Mergeable getMergeable(
			final Cell cell,
			final byte[] bytes ) {
		return PersistenceUtils.fromBinary(
				bytes,
				Mergeable.class);
	}

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
			System.err.println(
					rowCells.size());
			final Iterator<Cell> iter = rowCells.iterator();
			final Map<ByteArrayId, List<Cell>> postIterationMerges = new HashMap<>();
			// iterate once to capture individual tags/visibilities
			boolean rebuildList = false;
			while (iter.hasNext()) {
				final Cell cell = iter.next();
				// TODO consider avoiding extra byte array allocations
				final byte[] familyBytes = CellUtil.cloneFamily(
						cell);
				if (adapterIds.contains(
						new ByteArrayId(
								familyBytes))) {
					final byte[] tagsBytes = new byte[cell.getTagsLength()];
					// TODO consider avoiding extra byte array allocations
					CellUtil.copyTagTo(
							cell,
							tagsBytes,
							0);
					final ByteArrayId tags = new ByteArrayId(
							tagsBytes);
					List<Cell> cells = postIterationMerges.get(
							tags);
					if (cells == null) {
						cells = new ArrayList<>();
						postIterationMerges.put(
								tags,
								cells);
					}
					else {
						// this implies there is more than one cell with the
						// same vis, so merging will need to take place
						rebuildList = true;
					}
					cells.add(
							cell);
				}
			}
			if (rebuildList) {
				rowCells.clear();
				for (final Entry<ByteArrayId, List<Cell>> entry : postIterationMerges.entrySet()) {
					if (entry.getValue().size() > 1) {
						rowCells.add(
								mergeList(
										entry.getValue()));
					}
					else if (entry.getValue().size() == 1) {
						rowCells.add(
								entry.getValue().get(
										0));
					}
				}
			}
		}
		return true;
	}

	protected Cell mergeList(
			final List<Cell> cells ) {
		Mergeable currentMergeable = null;
		final Cell firstCell = cells.get(
				0);
		for (final Cell cell : cells) {
			final Mergeable mergeable = getMergeable(
					cell,
					// TODO consider avoiding extra byte array
					// allocations (which would require
					// persistence utils to be able to use
					// bytebuffer instead of byte[])
					CellUtil.cloneValue(
							cell));
			if (mergeable != null) {
				if (currentMergeable == null) {
					currentMergeable = mergeable;
				}
				else {
					currentMergeable.merge(
							mergeable);
				}
			}
		}
		final byte[] valueBinary = getBinary(
				currentMergeable);
		// this is basically a lengthy verbose form of cloning
		// in-place (without allocating new byte arrays) and
		// simply replacing the value with the new mergeable
		// value
		return new KeyValue(
				firstCell.getRowArray(),
				firstCell.getRowOffset(),
				firstCell.getRowLength(),
				firstCell.getFamilyArray(),
				firstCell.getFamilyOffset(),
				firstCell.getFamilyLength(),
				firstCell.getQualifierArray(),
				firstCell.getQualifierOffset(),
				firstCell.getQualifierLength(),
				firstCell.getTimestamp(),
				Type.codeToType(
						firstCell.getTypeByte()),
				valueBinary,
				0,
				valueBinary.length,
				firstCell.getTagsArray(),
				firstCell.getTagsOffset(),
				firstCell.getTagsLength());
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

	@Override
	public void preScannerOpen(
			final Scan scan ) {
		scan.setAttribute(
				OLD_MAX_VERSIONS_KEY,
				ByteBuffer
						.allocate(
								4)
						.putInt(
								scan.getMaxVersions())
						.array());
		scan.setMaxVersions();

	}
}
