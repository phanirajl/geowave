package mil.nga.giat.geowave.datastore.hbase.coprocessors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;

public class MergingRegionObserver extends
		BaseRegionObserver
{
	private final static Logger LOGGER = Logger.getLogger(
			MergingRegionObserver.class);

	public final static String COLUMN_FAMILIES_CONFIG_KEY = "hbase.coprocessor.merging.columnfamilies";

	// TEST ONLY!
	static {
		LOGGER.setLevel(
				Level.DEBUG);
	}

	private HashSet<String> mergingTables = new HashSet<>();
	private HashMap<String, MergeDataMessage> mergingTransformMap = new HashMap<>();
	//
	// @Override
	// public InternalScanner preFlushScannerOpen(
	// final ObserverContext<RegionCoprocessorEnvironment> e,
	// final Store store,
	// final KeyValueScanner memstoreScanner,
	// final InternalScanner s ) {
	// TableName tableName = e.getEnvironment().getRegionInfo().getTable();
	//
	// if (!tableName.isSystemTable()) {
	// String tableNameString = tableName.getNameAsString();
	//
	// if (mergingTables.contains(tableNameString)) {
	// LOGGER.debug(">>> preFlush for merging table: " + tableNameString);
	//
	// MergingInternalScanner mergingScanner = new MergingInternalScanner(
	// memstoreScanner);
	//
	// mergingScanner.setTransformMap(mergingTransformMap);
	//
	// return mergingScanner;
	// }
	// }
	//
	// return s;
	// }

	// @Override
	// public InternalScanner preCompactScannerOpen(
	// final ObserverContext<RegionCoprocessorEnvironment> e,
	// final Store store,
	// List<? extends KeyValueScanner> scanners,
	// final ScanType scanType,
	// final long earliestPutTs,
	// final InternalScanner s,
	// CompactionRequest request )
	// throws IOException {
	// TableName tableName = e.getEnvironment().getRegionInfo().getTable();
	//
	// if (!tableName.isSystemTable()) {
	// String tableNameString = tableName.getNameAsString();
	//
	// if (mergingTables.contains(tableNameString)) {
	// LOGGER.debug(">>> preCompact for merging table: " + tableNameString);
	//
	// MergingInternalScanner mergingScanner = new MergingInternalScanner(
	// scanners);
	//
	// mergingScanner.setTransformMap(mergingTransformMap);
	//
	// return mergingScanner;
	// }
	// }
	//
	// return s;
	// }

	// @Override
	// public boolean preScannerNext(
	// final ObserverContext<RegionCoprocessorEnvironment> e,
	// final InternalScanner s,
	// final List<Result> results,
	// final int limit,
	// final boolean hasMore )
	// throws IOException {
	// TableName tableName = e.getEnvironment().getRegionInfo().getTable();
	//
	// if (!tableName.isSystemTable()) {
	// String tableNameString = tableName.getNameAsString();
	//
	// if (mergingTables.contains(tableNameString)) {
	// LOGGER.debug(">>> preScannerNext for merging table: " +
	// tableName.getNameAsString());
	//
	// // Use merging scanner here
	// try (MergingInternalScanner mergingScanner = new MergingInternalScanner(
	// s)) {
	// mergingScanner.setTransformMap(mergingTransformMap);
	//
	// List<Cell> cellList = new ArrayList();
	// boolean notDone = mergingScanner.next(cellList);
	//
	// results.add(Result.create(cellList));
	//
	// e.bypass();
	//
	// return notDone;
	// }
	// }
	// }
	//
	// return hasMore;
	// }

	@Override
	public RegionScanner preScannerOpen(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final Scan scan,
			final RegionScanner s )
			throws IOException {
		TableName tableName = e.getEnvironment().getRegionInfo().getTable();
		// System.err.println(tableName.getNameAsString() + ": "
		// +e.getEnvironment().getTable(tableName).getConfiguration().get("test.me"));
		// System.err.println(tableName.getNameAsString() + ": "
		// +e.getEnvironment().getConfiguration().get("test.me"));

		// System.err.println(e.getEnvironment().getConfiguration().get("test.me"));
		if (!tableName.isSystemTable()) {
			if (scan != null) {
				// Filter scanFilter = scan.getFilter();
				// boolean mergeDataSet = false;
				// if (scanFilter != null) {
				// MergeDataMessage mergeDataMessage =
				// extractMergeData(scanFilter);
				//
				// if (mergeDataMessage != null) {
				// updateMergingColumnFamilies(mergeDataMessage);
				// mergeDataSet = true;
				// e.bypass();
				// e.complete();
				//
				// return null;
				// }
				// }
				// if(!mergeDataSet){
				// e.getEnvironment().getConfiguration().get("");
				if (tableName.getNameAsString().equals(
						"mil_nga_giat_geowave_test_SPATIAL_IDX")) {
					scan.setMaxVersions();
					Filter f;
					if (scan.getFilter() != null) {
						f = new FilterList(
								mdm,
								scan.getFilter());
					}
					else {
						f = mdm;
					}
					scan.setFilter(
							f);
				}
			}
		}
		// }

		return s;
	}

	MergeDataMessage mdm;

	@Override
	public void start(
			CoprocessorEnvironment e )
			throws IOException {
		String test = e.getConfiguration().get(
				"test.me");
		if (test != null) {
			try {
				mdm = MergeDataMessage.parseFrom(
						ByteArrayUtils.byteArrayFromString(
								test));
			}
			catch (DeserializationException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		super.start(
				e);
	}

	private MergeDataMessage extractMergeData(
			Filter checkFilter ) {
		if (checkFilter instanceof MergeDataMessage) {
			return (MergeDataMessage) checkFilter;
		}

		if (checkFilter instanceof FilterList) {
			for (Filter filter : ((FilterList) checkFilter).getFilters()) {
				MergeDataMessage mergingFilter = extractMergeData(
						filter);
				if (mergingFilter != null) {
					return mergingFilter;
				}
			}
		}

		return null;
	}

	private MergeDataMessage addMergeDataFilter(
			Filter checkFilter ) {
		if (checkFilter instanceof MergeDataMessage) {
			return (MergeDataMessage) checkFilter;
		}

		if (checkFilter instanceof FilterList) {
			for (Filter filter : ((FilterList) checkFilter).getFilters()) {
				MergeDataMessage mergingFilter = extractMergeData(
						filter);
				if (mergingFilter != null) {
					return mergingFilter;
				}
			}
		}

		return null;
	}

	private void updateMergingColumnFamilies(
			MergeDataMessage mergeDataMessage ) {
		LOGGER.debug(
				"Updating CF from message: " + mergeDataMessage.getAdapterId().getString());

		String tableName = mergeDataMessage.getTableName().getString();
		if (!mergingTables.contains(
				tableName)) {
			mergingTables.add(
					tableName);
		}

		if (!mergingTransformMap.containsKey(
				mergeDataMessage.getAdapterId())) {
			// RowTransform rowTransform = mergeDataMessage.getTransformData();
			//
			// try {
			// rowTransform.initOptions(mergeDataMessage.getOptions());
			// }
			// catch (IOException e) {
			// LOGGER.error(
			// "Error initializing row transform",
			// e);
			// }

			mergingTransformMap.put(
					tableName,
					mergeDataMessage);
		}
	}
}
