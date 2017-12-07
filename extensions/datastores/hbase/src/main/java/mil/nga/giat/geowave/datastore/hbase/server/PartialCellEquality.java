package mil.nga.giat.geowave.datastore.hbase.server;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;

public class PartialCellEquality
{
	private final Cell cell;
	private final boolean includeTags;

	public PartialCellEquality(
			final Cell cell,
			final boolean includeTags ) {
		this.cell = cell;
		this.includeTags = includeTags;
	}

	@Override
	public int hashCode() {
		final int rowHash = Bytes.hashCode(CellUtil.cloneRow(cell));
//				cell.getRowArray(),
//				cell.getRowOffset(),
//				cell.getRowLength());
		final int familyHash = Bytes.hashCode(
				cell.getFamilyArray(),
				cell.getFamilyOffset(),
				cell.getFamilyLength());
		final int qualifierHash = Bytes.hashCode(
				cell.getQualifierArray(),
				cell.getQualifierOffset(),
				cell.getQualifierLength());

		// combine the 6 sub-hashes
		int hash = (31 * familyHash) + qualifierHash;
		hash = (31*hash) + rowHash; 
		if (!includeTags) {
			return hash;
		}
		final int tagsHash = Bytes.hashCode(
				cell.getTagsArray(),
				cell.getTagsOffset(),
				cell.getTagsLength());
		return (31 * hash) + tagsHash;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final PartialCellEquality other = (PartialCellEquality) obj;
		return CellComparator.equalsRow(
				cell,
				other.cell) && Bytes.equals(CellUtil.cloneRow(cell), CellUtil.cloneRow(other.cell))&& CellComparator.equalsFamily(
				cell,
				other.cell)
				&& CellComparator.equalsQualifier(
						cell,
						other.cell)
				&& (!includeTags || tagsEqual(
						cell,
						other.cell));
	}

	protected static boolean tagsEqual(
			final Cell a,
			final Cell b ) {
		return Bytes.equals(
				a.getTagsArray(),
				a.getTagsOffset(),
				a.getTagsLength(),
				b.getTagsArray(),
				b.getTagsOffset(),
				b.getTagsLength());
	}
}
