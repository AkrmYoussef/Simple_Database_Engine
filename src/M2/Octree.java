package M2;

import M1.Methods;

import java.io.Serializable;
import java.sql.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import M1.DBApp;
import M1.DBAppException;
import M1.Page;
import M1.Row;

public class Octree implements Serializable {

	private int size; // sum of number of records in all leaves
	private OctPoint leftBackBottom; // this is edge number 0 in the "edge numbers" photo in github
	private OctPoint rightForwardUp; // this is edge number 7 in the "edge numbers" photo in github
	private OctPoint point; // this is null if the octree is a leaf
	private Octree[] children; // this is null if the octree is a leaf
	private HashMap<OctPoint, LinkedList<Integer>> records; // this is null if the octree is NOT a leaf
	private int maxNodeCapacity;

	public Octree(OctPoint leftBackBottom, OctPoint rightForwardUp, int maxNodeCapacity) { // constructor for a leaf
																							// octree, leaf octrees are
																							// converted to non-leaf
																							// octrees by the split
																							// method when their
																							// capacity is exceeded
		size = 0;
		this.leftBackBottom = leftBackBottom;
		this.rightForwardUp = rightForwardUp;
		this.maxNodeCapacity = maxNodeCapacity;
		records = new HashMap<OctPoint, LinkedList<Integer>>();
	}

	// fundamental methods
	public void insertPageIntoTree(Comparable x, Comparable y, Comparable z, Integer page) throws DBAppException {
		this.insertHelper(x, y, z, page, new LinkedList<Octree>());
	}

	public void deletePageFromTree(Comparable x, Comparable y, Comparable z, Integer page) throws DBAppException { // deletes
																													// ONLY
																													// one
																													// instance
																													// of
																													// the
																													// page,
																													// in
																													// case
																													// multiple
																													// instances
																													// of
																													// the
																													// same
																													// page
																													// exist
																													// in
																													// the
																													// same
																													// point
		this.deleteHelper(x, y, z, page, new LinkedList<Octree>());
	}

	public void updatePageAtPoint(Comparable x, Comparable y, Comparable z, Integer oldPage, Integer newPage)
			throws DBAppException {// replaces ONLY one instance of the page, in case multiple instances of the
									// same page exist in the same point
		validateNotNull(x, y, z);
		if (!this.pointExists(x, y, z))
			throw new DBAppException("Point " + x + " " + y + " " + z + " doesn't exist in the octree");
		deletePageFromTree(x, y, z, oldPage);
		insertPageIntoTree(x, y, z, newPage);
	}

	public LinkedList<Integer> getPagesAtPoint(Comparable x, Comparable y, Comparable z) throws DBAppException {
		validateNotNull(x, y, z);
		if (!this.pointExists(x, y, z))
			// throw new IllegalArgumentException("Point " + x + " " + y + " " + z + "
			// doesn't exist in the octree");
			return new LinkedList<Integer>(); // return empty list
		if (this.isLeaf())
			return records.get(new OctPoint(x, y, z));
		else {
			int position = Methods.getRelevantPosition(this.point.getX(), this.point.getY(), this.point.getZ(), x, y,
					z);
			return children[position].getPagesAtPoint(x, y, z);
		}
	}

	public LinkedList<Integer> getPagesAtPartialPoint(Comparable obj, Axis ax) {
		// partial Query with one point given
		LinkedList<Integer> pgs = new LinkedList<>();
		if (ax == Axis.X) {
			// obj is at point X, and search using X
			Set<Integer> setpages = ConcurrentHashMap.newKeySet();
			Methods2.fillSetWithPages_satisfyingCondition_forInputValue(this, setpages, "=", null, null, obj, null,
					null);
			pgs.addAll(setpages);

		} else if (ax == Axis.Y) {
			// obj is at point Y, and search using Y
			Set<Integer> setpages = ConcurrentHashMap.newKeySet();
			Methods2.fillSetWithPages_satisfyingCondition_forInputValue(this, setpages, null, "=", null, null, obj,
					null);
			pgs.addAll(setpages);

		} else {
			// obj is at point Z, and search using Z
			Set<Integer> setpages = ConcurrentHashMap.newKeySet();
			Methods2.fillSetWithPages_satisfyingCondition_forInputValue(this, setpages, null, null, "=", null, null,
					obj);
			pgs.addAll(setpages);

		}
		return pgs;
	}

	public LinkedList<Integer> getPagesAtPartialPoint(Comparable obj1, Axis ax1, Comparable obj2, Axis ax2) {
		// partial Query with two points given
		LinkedList<Integer> pgs = new LinkedList<>();
		if (ax1 == Axis.X && ax2 == Axis.Y) {
			// obj is at point XY, and search using XY
			Set<Integer> setpages = ConcurrentHashMap.newKeySet();
			Methods2.fillSetWithPages_satisfyingCondition_forInputValue(this, setpages, "=", "=", null, obj1, obj2,
					null);
			pgs.addAll(setpages);

		} else if (ax1 == Axis.X && ax2 == Axis.Z) {
			// obj is at point XZ, and search using XZ
			Set<Integer> setpages = ConcurrentHashMap.newKeySet();
			Methods2.fillSetWithPages_satisfyingCondition_forInputValue(this, setpages, "=", null, "=", obj1, null,
					obj2);
			pgs.addAll(setpages);

		} else {
			// obj is at point YZ, and search using YZ
			Set<Integer> setpages = ConcurrentHashMap.newKeySet();
			Methods2.fillSetWithPages_satisfyingCondition_forInputValue(this, setpages, null, "=", "=", null, obj1,
					obj2);
			pgs.addAll(setpages);

		}
		// no other combinations assuming X should be before Y and before Z in input
		return pgs;
	}

	public boolean pointExists(Comparable x, Comparable y, Comparable z) throws DBAppException { // checks existence in
																									// the leaf level
																									// only
		validatePointIsInTreeBounday(x, y, z);
		if (this.isLeaf())
			return records.containsKey(new OctPoint(x, y, z));
		else {
			int position = Methods.getRelevantPosition(this.point.getX(), this.point.getY(), this.point.getZ(), x, y,
					z);
			return children[position].pointExists(x, y, z);
		}

	}

	public int getSize() {
		return size;
	}

	public void printTree() {
		printTreeHelper(0);
	}

//// below are helper private methods ////
	private void insertHelper(Comparable x, Comparable y, Comparable z, Integer page, LinkedList<Octree> traversedSoFar)
			throws DBAppException {
		validateNotNull(x, y, z);
		validatePointIsInTreeBounday(x, y, z);
		traversedSoFar.add(this);
		if (!this.isLeaf()) {
			int position = Methods.getRelevantPosition(this.point.getX(), this.point.getY(), this.point.getZ(), x, y,
					z);
			children[position].insertHelper(x, y, z, page, traversedSoFar);
		} else {
			OctPoint PointToInsert = new OctPoint(x, y, z);
			if (!records.containsKey(PointToInsert)) {
				LinkedList<Integer> pages = new LinkedList<Integer>();
				pages.add(page);
				records.put(PointToInsert, pages);
				for (Octree octree : traversedSoFar)
					octree.size++;
			} else {
				records.get(PointToInsert).add(page);
				// no need to increment size because we now inserted a duplicate page,which is
				// not counted in the size
			}

			if (records.size() > maxNodeCapacity)
				this.split();

		}
	}

	private void deleteHelper(Comparable x, Comparable y, Comparable z, Integer page, LinkedList<Octree> traversedSoFar)
			throws DBAppException {
		validateNotNull(x, y, z);
		if (!this.pointExists(x, y, z))
			throw new DBAppException("'IllegalArguments in Octree': PointToDelete " + x + " " + y + " " + z
					+ " doesn't exist in the octree");
		traversedSoFar.addLast(this);
		if (!this.isLeaf()) {
			int position = Methods.getRelevantPosition(this.point.getX(), this.point.getY(), this.point.getZ(), x, y,
					z);
			children[position].deleteHelper(x, y, z, page, traversedSoFar);
		} else {
			OctPoint PointToDelete = new OctPoint(x, y, z);
			LinkedList<Integer> pages = records.get(PointToDelete);
			boolean succefullyDeleted = pages.remove(page); // remove page from list of pages
			if (!succefullyDeleted)
				throw new DBAppException("'IllegalArguments in Octree': the input page " + page
						+ "  was not found in the list of pages of Point " + x + " " + y + " " + z + "");
			if (pages.size() == 0) // if the list became empty, remove the point with its empty list from the
									// records
			{
				records.remove(PointToDelete);
				for (Octree octree : traversedSoFar)
					octree.size--;
				// merge all leaves with parent if parent size became not more than
				// maxNodeCapacity
				traversedSoFar.removeLast(); // remove the current leaf from the list of parents
				while (!traversedSoFar.isEmpty()) {
					Octree directParent = traversedSoFar.removeLast();
					if (directParent.size <= maxNodeCapacity)
						directParent.devourChildren();
					else
						break; // if the direct parent size is still more than maxNodeCapacity,then so are all
								// other non-direct parents
				}
			}

		}
	}

	private void devourChildren() {
		if (this.isLeaf())
			return;
		this.records = new HashMap<OctPoint, LinkedList<Integer>>(); // preparing a non-leaf octree to be a leaf octree
		for (Octree child : this.children) {
			child.devourChildren();
			for (OctPoint key : child.records.keySet()) {
				LinkedList<Integer> value = child.records.get(key);
				this.records.put(key, value);
			}
		}
		this.children = null;
		this.point = null;

	}

	private void split() throws DBAppException {

		Comparable Xsmall = this.leftBackBottom.getX();
		Comparable Ysmall = this.leftBackBottom.getY();
		Comparable Zsmall = this.leftBackBottom.getZ();

		Comparable Xbig = this.rightForwardUp.getX();
		Comparable Ybig = this.rightForwardUp.getY();
		Comparable Zbig = this.rightForwardUp.getZ();

		Comparable xCenter = Methods.getMiddleValue(Xsmall, Xbig);
		Comparable yCenter = Methods.getMiddleValue(Ysmall, Ybig);
		Comparable zCenter = Methods.getMiddleValue(Zsmall, Zbig);

		this.point = new OctPoint(xCenter, yCenter, zCenter);
		children = new Octree[8];

		children[0] = new Octree(new OctPoint(Xsmall, Ysmall, Zsmall), new OctPoint(xCenter, yCenter, zCenter),
				maxNodeCapacity);
		children[1] = new Octree(new OctPoint(Xsmall, Ysmall, zCenter), new OctPoint(xCenter, yCenter, Zbig),
				maxNodeCapacity);
		children[2] = new Octree(new OctPoint(Xsmall, yCenter, Zsmall), new OctPoint(xCenter, Ybig, zCenter),
				maxNodeCapacity);
		children[3] = new Octree(new OctPoint(Xsmall, yCenter, zCenter), new OctPoint(xCenter, Ybig, Zbig),
				maxNodeCapacity);
		children[4] = new Octree(new OctPoint(xCenter, Ysmall, Zsmall), new OctPoint(Xbig, yCenter, zCenter),
				maxNodeCapacity);
		children[5] = new Octree(new OctPoint(xCenter, Ysmall, zCenter), new OctPoint(Xbig, yCenter, Zbig),
				maxNodeCapacity);
		children[6] = new Octree(new OctPoint(xCenter, yCenter, Zsmall), new OctPoint(Xbig, Ybig, zCenter),
				maxNodeCapacity);
		children[7] = new Octree(new OctPoint(xCenter, yCenter, zCenter), new OctPoint(Xbig, Ybig, Zbig),
				maxNodeCapacity);

		for (OctPoint recordPoint : records.keySet()) // transfer the records to the children
		{
			LinkedList<Integer> recordPages = records.get(recordPoint);
			int childPositionToInsertInto = Methods.getRelevantPosition(this.point.getX(), this.point.getY(),
					this.point.getZ(), recordPoint.getX(), recordPoint.getY(), recordPoint.getZ());
			for (Integer page : recordPages) {
				LinkedList<Octree> traversedSoFar = new LinkedList<Octree>();
				children[childPositionToInsertInto].insertHelper(recordPoint.getX(), recordPoint.getY(),
						recordPoint.getZ(), page, traversedSoFar);
			}
		}
		records = null; // after all the records are transferred to the children, the records are no
						// longer needed in the parent

	}

	private void validateNotNull(Comparable x, Comparable y, Comparable z) throws DBAppException {
		if (x == null)
			throw new DBAppException("'IllegalArguments in Octree': x axis value can't be null");
		if (y == null)
			throw new DBAppException("'IllegalArguments in Octree': y axis value can't be null");
		if (z == null)
			throw new DBAppException("'IllegalArguments in Octree': z axis value can't be null");

	}

	private void validatePointIsInTreeBounday(Comparable x, Comparable y, Comparable z) throws DBAppException {
		boolean xIsInBoundary = (x.compareTo(this.leftBackBottom.getX()) >= 0)
				&& (x.compareTo(this.rightForwardUp.getX()) <= 0);
		boolean yIsInBoundary = (y.compareTo(this.leftBackBottom.getY()) >= 0)
				&& (y.compareTo(this.rightForwardUp.getY()) <= 0);
		boolean zIsInBoundary = (z.compareTo(this.leftBackBottom.getZ()) >= 0)
				&& (z.compareTo(this.rightForwardUp.getZ()) <= 0);
		if (!xIsInBoundary || !yIsInBoundary || !zIsInBoundary)
			throw new DBAppException("'IllegalArguments in Octree': Point " + x + " " + y + " " + z
					+ " can't exist in the tree because it is not even between " + leftBackBottom + " and "
					+ rightForwardUp + " !");
	}

	private void printTreeHelper(int curLevel) { // to visualize the output, each level has a unique indentation before
													// it

		String levelSpaces = createSpaces(curLevel * 5);
		if (this.isLeaf()) {
			if (this.size > 0) {
				System.out.print(levelSpaces + "level " + curLevel + " Leaf " + this.leftBackBottom + " "
						+ this.rightForwardUp + " " + "no.records : " + this.size + " ");
				System.out.print(createSpaces(curLevel + 1));
				for (OctPoint key : this.records.keySet())
					System.out.print("key : " + key + " " + "list size : " + this.records.get(key).size() + "    ,");
				System.out.println();
			}
		} else {
			System.out.println(levelSpaces + "level " + curLevel + " Non-leaf " + this.leftBackBottom + " "
					+ this.rightForwardUp + " size " + this.size);
			for (int i = 0; i < children.length; i++) {
				children[i].printTreeHelper(curLevel + 1);
			}
		}

	}

	private String createSpaces(int spaces) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < spaces; i++)
			sb.append(" ");
		return sb.toString();

	}

	// getters
	public OctPoint getLeftBackBottom() {
		return leftBackBottom;
	}

	public OctPoint getRightForwardUp() {
		return rightForwardUp;
	}

	public OctPoint getPoint() {
		return point;
	}

	public HashMap<OctPoint, LinkedList<Integer>> getRecords() {
		return records;
	}

	public Octree[] getChildren() {
		return children;
	}

	public boolean isLeaf() {
		return (children == null);
	}

	public void printOctree() {
		printOctreeHelper(this, "");
	}

	private void printOctreeHelper(Octree node, String prefix) {
		if (node.isLeaf()) {
			System.out.println(prefix + "Leaf node with " + node.getRecords().size() + " records:");
			for (Map.Entry<OctPoint, LinkedList<Integer>> entry : node.getRecords().entrySet()) {
				System.out.println(prefix + "- " + entry.getKey() + ": " + entry.getValue());
			}
		} else {
			System.out.println(prefix + "Non-leaf node:");
			for (int i = 0; i < 8; i++) {
				if (node.getChildren()[i] != null) {
					System.out.println(prefix + "- " + i + ":");
					printOctreeHelper(node.getChildren()[i], prefix + "  ");
				}
			}
		}
	}

	public static void main(String[] args) throws DBAppException {

	}
}
