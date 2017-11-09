package mil.nga.giat.geowave.datastore.hbase.coprocessors;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;

public class MergingCombinerFilter extends
		FilterBase
{
	private final static Logger LOGGER = Logger.getLogger(MergingCombinerFilter.class);

	private RowTransform transformData;
	private HashMap<String, String> options;

	public MergingCombinerFilter() {

	}

	@Override
	public boolean hasFilterRow() {
		return true;
	}

	@Override
	public ReturnCode filterKeyValue(
			Cell v )
			throws IOException {
		return ReturnCode.INCLUDE;
	}

	@Override
	public void filterRowCells(
			List<Cell> ignored )
			throws IOException {
		if (ignored.size() < 2){
			return;
		}
		Cell mergedResult = ignored.get(0);
		for (int i = 1; i < ignored.size(); i++){
			mergedResult = mergeCells(mergedResult, ignored.get(i));
		}
		ignored.clear();
		ignored.add(mergedResult);
	}

	public static MergingCombinerFilter parseFrom(
			final byte[] pbBytes )
			throws DeserializationException {
		final ByteBuffer buf = ByteBuffer.wrap(pbBytes);

		final int tableLength = buf.getInt();
		final int adapterLength = buf.getInt();
		final int transformLength = buf.getInt();

		final byte[] tableBytes = new byte[tableLength];
		buf.get(tableBytes);

		final byte[] adapterBytes = new byte[adapterLength];
		buf.get(adapterBytes);

		final byte[] transformBytes = new byte[transformLength];
		buf.get(transformBytes);

		final byte[] optionsBytes = new byte[pbBytes.length - transformLength - adapterLength - tableLength - 12];
		buf.get(optionsBytes);

		MergingCombinerFilter mergingFilter = new MergingCombinerFilter();

		mergingFilter.setAdapterId(new ByteArrayId(
				adapterBytes));

		RowTransform rowTransform = PersistenceUtils.fromBinary(
				transformBytes,
				RowTransform.class);
		HashMap<String, String> options = (HashMap<String, String>) SerializationUtils.deserialize(optionsBytes);

		try {
			rowTransform.initOptions(options);
		}
		catch (IOException e) {
			LOGGER.error(
					"Error initializing row transform",
					e);
		}
		mergingFilter.setTransformData(rowTransform);

		mergingFilter.setOptions(options);
		
		return mergingFilter;
	}

	@Override
	public byte[] toByteArray()
			throws IOException {
		final byte[] tableBinary = tableName.getBytes();
		final byte[] adapterBinary = adapterId.getBytes();
		final byte[] transformBinary = PersistenceUtils.toBinary(transformData);
		final byte[] optionsBinary = SerializationUtils.serialize(options);

		final ByteBuffer buf = ByteBuffer.allocate(tableBinary.length + adapterBinary.length + transformBinary.length
				+ optionsBinary.length + 12);

		buf.putInt(tableBinary.length);
		buf.putInt(adapterBinary.length);
		buf.putInt(transformBinary.length);
		buf.put(tableBinary);
		buf.put(adapterBinary);
		buf.put(transformBinary);
		buf.put(optionsBinary);

		return buf.array();
	}
	
	public RowTransform getTransformData() {
		return transformData;
	}

	public void setTransformData(
			RowTransform transformData ) {
		this.transformData = transformData;
	}

	public HashMap<String, String> getOptions() {
		return options;
	}

	public void setOptions(
			HashMap<String, String> options ) {
		this.options = options;
	}

	/**
	 * Assumes cells share family and qualifier
	 * 
	 * @param cell1
	 * @param cell2
	 * @return
	 */
	private Cell mergeCells(
			Cell cell1,
			Cell cell2 ) {
		Cell mergedCell = null;

		ByteArrayId family = new ByteArrayId(
				CellUtil.cloneFamily(cell1));

		if (adapterId.equals(family)) {

			final Mergeable mergeable = transformData.getRowAsMergeableObject(
					family,
					new ByteArrayId(
							CellUtil.cloneQualifier(cell1)),
					CellUtil.cloneValue(cell1));

			if (mergeable != null) {
				final Mergeable otherMergeable = transformData.getRowAsMergeableObject(
						family,
						new ByteArrayId(
								CellUtil.cloneQualifier(cell1)),
						CellUtil.cloneValue(cell2));

				if (otherMergeable != null) {
					mergeable.merge(otherMergeable);
				}
				else {
					LOGGER.error("Cell value is not Mergeable!");
				}

				mergedCell = copyCell(
						cell1,
						transformData.getBinaryFromMergedObject(mergeable));
			}
			else {
				LOGGER.error("Cell value is not Mergeable!");
			}
		}
		else {
			LOGGER.error("No merging transform for adapter: " + family.getString());
		}

		return mergedCell;
	}
	private static Cell copyCell(
			Cell sourceCell,
			byte[] newValue ) {
		if (newValue == null) {
			newValue = CellUtil.cloneValue(sourceCell);
		}

		Cell newCell = CellUtil.createCell(
				CellUtil.cloneRow(sourceCell),
				CellUtil.cloneFamily(sourceCell),
				CellUtil.cloneQualifier(sourceCell),
				sourceCell.getTimestamp(),
				KeyValue.Type.Put.getCode(),
				newValue);

		return newCell;
	}
}
