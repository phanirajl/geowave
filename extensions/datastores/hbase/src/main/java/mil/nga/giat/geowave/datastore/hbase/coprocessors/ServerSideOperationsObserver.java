package mil.nga.giat.geowave.datastore.hbase.coprocessors;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableSet;

import mil.nga.giat.geowave.core.store.server.ServerOpConfig.ServerOpScope;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;

public class ServerSideOperationsObserver extends
		BaseRegionObserver
{
	private final static Logger LOGGER = Logger.getLogger(
			ServerSideOperationsObserver.class);
	public static final String SERVER_OP_PREFIX = "serverop";
	public static final String SERVER_OP_SCOPES_KEY = "scopes";
	public static final String SERVER_OP_OPTIONS_PREFIX = "options";
	public static final String SERVER_OP_CLASS_KEY = "class";
	public static final String SERVER_OP_PRIORITY_KEY = "priority";

	private ServerSideOperationStore opStore = null;

	@Override
	public InternalScanner preFlushScannerOpen(
			final ObserverContext<RegionCoprocessorEnvironment> c,
			final Store store,
			final KeyValueScanner memstoreScanner,
			final InternalScanner s )
			throws IOException {
		// TODO Auto-generated method stub
		return super.preFlushScannerOpen(
				c,
				store,
				memstoreScanner,
				s);
	}

	@Override
	public InternalScanner preCompactScannerOpen(
			final ObserverContext<RegionCoprocessorEnvironment> c,
			final Store store,
			final List<? extends KeyValueScanner> scanners,
			final ScanType scanType,
			final long earliestPutTs,
			final InternalScanner s )
			throws IOException {
		// TODO Auto-generated method stub
		return super.preCompactScannerOpen(
				c,
				store,
				scanners,
				scanType,
				earliestPutTs,
				s);
	}

	@Override
	public RegionScanner preScannerOpen(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final Scan scan,
			final RegionScanner s )
			throws IOException {
		if (opStore == null) {
			return super.preScannerOpen(
					e,
					scan,
					s);
		}
		final TableName tableName = e.getEnvironment().getRegionInfo().getTable();
		final String namespace = tableName.getNamespaceAsString();
		final String qualifier = tableName.getQualifierAsString();
		if (!tableName.isSystemTable()) {
			if (scan != null) {
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

		return super.preScannerOpen(
				e,
				scan,
				s);
	}

	@Override
	public void start(
			final CoprocessorEnvironment e )
			throws IOException {
		opStore = new ServerSideOperationStore();
		final Configuration config = e.getConfiguration();
		final Map<String, String> serverOpProperties = config.getValByRegex(
				SERVER_OP_PREFIX);
		final Set<String> uniqueOps = new HashSet<>();
		for (final String key : serverOpProperties.keySet()) {
			final int index = StringUtils.ordinalIndexOf(
					key,
					".",
					4);
			if (index > 0) {
				uniqueOps.add(
						key.substring(
								0,
								index + 1));
			}
		}

		for (final String uniqueOp : uniqueOps) {
			final String priorityStr = config.get(
					String.format(
							"%s%s",
							uniqueOp,
							SERVER_OP_PRIORITY_KEY));
			if ((priorityStr == null) || priorityStr.isEmpty()) {
				LOGGER.warn(
						"Skipping server op - unable to find priority for '" + uniqueOp + "'");
				continue;
			}
			final int priority = Integer.parseInt(
					priorityStr);
			final String commaDelimitedScopes = config.get(
					String.format(
							"%s%s",
							uniqueOp,
							SERVER_OP_SCOPES_KEY));
			if ((commaDelimitedScopes == null) || commaDelimitedScopes.isEmpty()) {
				LOGGER.warn(
						"Skipping server op - unable to find scopes for '" + uniqueOp + "'");
				continue;
			}
			final ImmutableSet<ServerOpScope> scopes = HBaseUtils.stringToScopes(
					commaDelimitedScopes);
			final String className = config.get(
					String.format(
							"%s%s",
							uniqueOp,
							SERVER_OP_CLASS_KEY));
			if ((className == null) || className.isEmpty()) {
				LOGGER.warn(
						"Skipping server op - unable to find priority for '" + uniqueOp + "'");
				continue;
			}
			final String optionsPrefix = String.format(
					"%s%s.",
					uniqueOp,
					SERVER_OP_OPTIONS_PREFIX);
			final Map<String, String> optionsLongKeys = config.getValByRegex(
					optionsPrefix);
			final Map<String, String> optionsShortenedKeys = new HashMap<>();
			final int shortenedIndex = optionsPrefix.length();
			for (final Entry<String, String> entry : optionsLongKeys.entrySet()) {
				optionsShortenedKeys.put(
						entry.getKey().substring(
								shortenedIndex),
						entry.getValue());
			}

		}
		super.start(
				e);
	}

	private void updateMergingColumnFamilies(
			final MergingCombinerFilter mergeDataMessage ) {
		LOGGER.debug(
				"Updating CF from message: " + mergeDataMessage.getAdapterId().getString());

		final String tableName = mergeDataMessage.getTableName().getString();
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
