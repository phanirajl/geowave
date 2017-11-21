package mil.nga.giat.geowave.datastore.hbase.coprocessors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableSet;

import mil.nga.giat.geowave.core.store.server.ServerOpConfig.ServerOpScope;
import mil.nga.giat.geowave.datastore.hbase.server.HBaseServerOp;
import mil.nga.giat.geowave.datastore.hbase.server.ServerOpInternalScannerWrapper;
import mil.nga.giat.geowave.datastore.hbase.server.ServerOpRegionScannerWrapper;
import mil.nga.giat.geowave.datastore.hbase.server.ServerSideOperationStore;
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
	private static final int SERVER_OP_OPTIONS_PREFIX_LENGTH = SERVER_OP_OPTIONS_PREFIX.length();

	private ServerSideOperationStore opStore = null;
	private static final RegionScannerWrapperFactory REGION_SCANNER_FACTORY = new RegionScannerWrapperFactory();
	private static final InternalScannerWrapperFactory INTERNAL_SCANNER_FACTORY = new InternalScannerWrapperFactory();

	private static interface ScannerWrapperFactory<T extends InternalScanner>
	{
		public T createScannerWrapper(
				Collection<HBaseServerOp> orderedServerOps,
				T delegate );
	}

	private static class RegionScannerWrapperFactory implements
			ScannerWrapperFactory<RegionScanner>
	{

		@Override
		public RegionScanner createScannerWrapper(
				final Collection<HBaseServerOp> orderedServerOps,
				final RegionScanner delegate ) {
			return new ServerOpRegionScannerWrapper(
					orderedServerOps,
					delegate);
		}
	}

	private static class InternalScannerWrapperFactory implements
			ScannerWrapperFactory<InternalScanner>
	{

		@Override
		public InternalScanner createScannerWrapper(
				final Collection<HBaseServerOp> orderedServerOps,
				final InternalScanner delegate ) {
			return new ServerOpInternalScannerWrapper(
					orderedServerOps,
					delegate);
		}
	}

	@Override
	public InternalScanner preFlushScannerOpen(
			final ObserverContext<RegionCoprocessorEnvironment> c,
			final Store store,
			final KeyValueScanner memstoreScanner,
			final InternalScanner s )
			throws IOException {
		if (opStore == null) {
			return super.preFlushScannerOpen(
					c,
					store,
					memstoreScanner,
					s);
		}
		return super.preFlushScannerOpen(
				c,
				store,
				memstoreScanner,
				wrapScannerWithOps(
						c.getEnvironment().getRegionInfo().getTable(),
						s,
						ServerOpScope.MINOR_COMPACTION,
						INTERNAL_SCANNER_FACTORY));
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
		if (opStore == null) {
			return super.preCompactScannerOpen(
					c,
					store,
					scanners,
					scanType,
					earliestPutTs,
					s);
		}

		return super.preCompactScannerOpen(
				c,
				store,
				scanners,
				scanType,
				earliestPutTs,
				wrapScannerWithOps(
						c.getEnvironment().getRegionInfo().getTable(),
						s,
						ServerOpScope.MAJOR_COMPACTION,
						INTERNAL_SCANNER_FACTORY));
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

		return super.preScannerOpen(
				e,
				scan,
				wrapScannerWithOps(
						e.getEnvironment().getRegionInfo().getTable(),
						s,
						ServerOpScope.SCAN,
						REGION_SCANNER_FACTORY));
	}

	public <T extends InternalScanner> T wrapScannerWithOps(
			final TableName tableName,
			final T scanner,
			final ServerOpScope scope,
			final ScannerWrapperFactory<T> factory ) {
		if (!tableName.isSystemTable()) {
			final String namespace = tableName.getNamespaceAsString();
			final String qualifier = tableName.getQualifierAsString();
			final Collection<HBaseServerOp> orderedServerOps = opStore.getOperations(
					namespace,
					qualifier,
					scope);
			if (!orderedServerOps.isEmpty()) {
				return factory.createScannerWrapper(
						orderedServerOps,
						scanner);
			}
		}
		return scanner;
	}

	@Override
	public void start(
			final CoprocessorEnvironment e )
			throws IOException {
		opStore = new ServerSideOperationStore();
		final Configuration config = e.getConfiguration();
		final Map<String, List<String>> uniqueOpsWithOptionKeys = new HashMap<>();
		for (final Map.Entry<String, String> entry : config) {
			if (entry.getKey().startsWith(
					SERVER_OP_PREFIX)) {
				final String key = entry.getKey();
				final int index = StringUtils.ordinalIndexOf(
						key,
						".",
						4);
				if (index > 0) {
					final String uniqueOp = key.substring(
							0,
							index + 1);
					List<String> optionKeys = uniqueOpsWithOptionKeys.get(
							uniqueOp);
					if (optionKeys == null) {
						optionKeys = new ArrayList<>();
						uniqueOpsWithOptionKeys.put(
								uniqueOp,
								optionKeys);
					}
					if (key.length() > (uniqueOp.length() + 1 + SERVER_OP_OPTIONS_PREFIX_LENGTH)) {
						if (key.substring(
								uniqueOp.length(),
								uniqueOp.length() + SERVER_OP_OPTIONS_PREFIX_LENGTH).equals(
										SERVER_OP_OPTIONS_PREFIX)) {
							optionKeys.add(
									key.substring(
											uniqueOp.length() + 1 + SERVER_OP_OPTIONS_PREFIX_LENGTH));
						}
					}
				}
			}
		}

		for (final Entry<String, List<String>> uniqueOpAndOptions : uniqueOpsWithOptionKeys.entrySet()) {
			final String uniqueOp = uniqueOpAndOptions.getKey();
			final String priorityStr = config.get(
					uniqueOp + SERVER_OP_PRIORITY_KEY);
			if ((priorityStr == null) || priorityStr.isEmpty()) {
				LOGGER.warn(
						"Skipping server op - unable to find priority for '" + uniqueOp + "'");
				continue;
			}
			final int priority = Integer.parseInt(
					priorityStr);
			final String commaDelimitedScopes = config.get(
					uniqueOp + SERVER_OP_SCOPES_KEY);
			if ((commaDelimitedScopes == null) || commaDelimitedScopes.isEmpty()) {
				LOGGER.warn(
						"Skipping server op - unable to find scopes for '" + uniqueOp + "'");
				continue;
			}
			final ImmutableSet<ServerOpScope> scopes = HBaseUtils.stringToScopes(
					commaDelimitedScopes);
			final String className = config.get(
					uniqueOp + SERVER_OP_CLASS_KEY);
			if ((className == null) || className.isEmpty()) {
				LOGGER.warn(
						"Skipping server op - unable to find priority for '" + uniqueOp + "'");
				continue;
			}
			final List<String> optionKeys = uniqueOpAndOptions.getValue();
			final Map<String, String> optionsMap = new HashMap<>();
			for (final String optionKey : optionKeys) {
				final String optionValue = config.get(
						uniqueOp + SERVER_OP_OPTIONS_PREFIX + "." + optionKey);
				optionsMap.put(
						optionKey,
						optionValue);
			}
			final String[] uniqueOpSplit = uniqueOp.split(
					"\\.");
			opStore.addOperation(
					uniqueOpSplit[1],
					uniqueOpSplit[2],
					uniqueOpSplit[3],
					priority,
					scopes,
					className,
					optionsMap);
		}
		super.start(
				e);
	}
}
