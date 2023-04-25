package M1;
import java.io.Serializable;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;
import java.util.function.UnaryOperator;
import java.util.Optional;

public class Page implements Serializable {
     private String tblBelongTo;
     private int noOfCurrentRows;
     private int pid ;  /**UPADTE**: page id is now an integer representing 
                                  the id of the page on the disk (.ser file)
                             and NOT the index of the page in the pages vector */
     private Object maxValInThePage;// used to sort a page according to PK
     private Object minValInThePage;
     private Vector<Row> data;  
     private String path;

    public Page(String strTableName , int pid ){
        tblBelongTo = strTableName;
        this.pid =pid;
        noOfCurrentRows =0;
        data = new Vector<>();
        path = "src/resources/tables/" + strTableName + "/pages/page" + pid +".ser";
        DBApp.serialize(path,data);

    }

    public void insertAnEntry(Row entry) throws DBAppException {
        DBApp.serialize(path,data);
        if (data.contains(entry))
            throw new DBAppException("This entry already exists");
        data.add(entry);
        noOfCurrentRows++;
        Collections.sort(data);
    	if(!isEmpty()) {
    		minValInThePage = data.get(0).getData().get(0); // minValueOfThPage is the primary key value of the first tuple
        	maxValInThePage = data.get(data.size()-1).getData().get(0);// maxValueOfThPage is the primary key value of the last tuple
    	}
        DBApp.serialize(path,data);
    }
    
    public void deleteEntry(Row entry) throws DBAppException {
    	DBApp.serialize(path, data);
    	if(entry != null) {
    		for (Row row : data) {
				if(row.compareTo(entry) == 0) {
					data.remove(row);
					break;
				}
			}
    		setNoOfCurrentRowsBYOFFSET(-1);
    		if(!isEmpty()) {
    			minValInThePage = data.get(0).getData().get(0); // minValueOfThPage is the primary key value of the first tuple
    			maxValInThePage = data.get(data.size()-1).getData().get(0);// maxValueOfThPage is the primary key value of the last tuple
    		}
    		DBApp.serialize(path,data);
    	}
    	else {
			throw new DBAppException("You cannot delete a non existent row");
		}
	}
    
    public Object getMaxValInThePage() {
        return maxValInThePage;
    }

    public Object getMinValInThePage() {
        return minValInThePage;
    }

    public int getNoOfCurrentRows() {
        return data.size();
    }

    public void setNoOfCurrentRows(int noOfCurrentRows) {
        this.noOfCurrentRows = noOfCurrentRows;
    }
    public void setNoOfCurrentRowsBYOFFSET(int offset) {
		this.noOfCurrentRows += offset;
	}

    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }

    public Vector<Row> getData() {
        return data;
    }

    public void setData(Vector<Row> data) {
        this.data = data;
    }

    public String getTblBelongTo() {
        return tblBelongTo;
    }

    public void setTblBelongTo(String tblBelongTo) {
        this.tblBelongTo = tblBelongTo;
    }
    public String toString(){
        String output = "";
        for (Row x:data) {
            output += x.toString() + "\n";
        }

        return output;
    }
    
    public boolean isFull() {
		return DBApp.MaximumRowsCountinTablePage <= getNoOfCurrentRows();
	}
    public boolean isEmpty() {
		return data.isEmpty();
	}

    public void updateRow(Table table ,Row entry, Hashtable<String, Object> htblColNameData) {
        DBApp.serialize(path, data);
    	for (Row row : data) {
            if(row.compareTo(entry) == 0) {
                Enumeration<String> strEnumeration = htblColNameData.keys();
                while (strEnumeration.hasMoreElements()) {
                    String strColName = strEnumeration.nextElement();
                    Object objColValue = htblColNameData.get(strColName);
                    row.getData().set(table.getColumnEquivalentIndex(strColName), objColValue);
                }


                break;
            }
        }        
    }
}