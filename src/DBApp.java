import java.io.*;
import java.text.ParseException;
import java.util.*;

public class DBApp  {
    public int MaximumRowsCountinTablePage;
    public int MaximumEntriesinOctreeNode;
    private LinkedList<String> listofCreatedTables;
    public DBApp(){
        listofCreatedTables = new LinkedList<>();
init();
    }

    public void init( ){
     this.readConfig();
    };
    // create table and its related methods
    public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType,
                            Hashtable<String,String> htblColNameMin,Hashtable<String,String> htblColNameMax )throws DBAppException{
       validation(htblColNameType,htblColNameMin,htblColNameMax);
        // validation method checks if the type of table's columns is one of the 4 types specified in the description
        // validation also checks if any column does not have maxVal or minVal , hence throws exception
        readConfig();
        Table tblCreated = new Table(strTableName,strClusteringKeyColumn,htblColNameType,htblColNameMin,htblColNameMax,MaximumRowsCountinTablePage);
        listofCreatedTables.add(strTableName);
        String strTablePath = "src/resources/tables/" + strTableName;
        File newFile = new File(strTablePath);
        newFile.mkdir();
        newFile = new File(strTablePath+ "/pages");
        newFile.mkdir();
        //serialization
        serialize(strTablePath + "/" + strTableName +".ser" , tblCreated);
    }
    private static void validation( Hashtable<String,String> htblColNameType,Hashtable<String,String> htblColNameMin,
                                    Hashtable<String,String> htblColNameMax ) throws DBAppException{

        Enumeration<String> strEnumeration = htblColNameType.keys();
        while ( strEnumeration.hasMoreElements()){
            String strColName = strEnumeration.nextElement();
            String strColType = htblColNameType.get(strColName);


            if(!(strColType.equals("java.lang.Integer") || strColType.equals("java.lang.String") ||
               strColType.equals("java.lang.Double") || strColType.equals("java.util.Date")))
                throw new DBAppException("The type " + strColType +" is not supported");

            if(!htblColNameMin.containsKey(strColName) || !htblColNameMax.containsKey(strColName)) {
                throw new DBAppException("The column "+strColName+" must exist");

            }

        }

    }

    public void readConfig(){
        /*
         this method objective is to read the DBApp configuration file in order to extract data needed
         for setting the instance variables of the DBApp Class (MaximumRowsCountinTablePage,MaximumEntriesinOctreeNode)
         */
        String filePath = "src/resources/DBApp.config";

        try{
            FileReader fr = new FileReader(filePath); // throws FileNotFoundException
            BufferedReader br = new BufferedReader(fr);
            String readingOut1stLine = br.readLine(); // throws IOException
            // max rows in a table page
            StringTokenizer strTokenizer = new StringTokenizer(readingOut1stLine);
             while (strTokenizer.hasMoreElements()){
                 String tmp = strTokenizer.nextToken();
                 if(tmp.equals("="))
                     MaximumRowsCountinTablePage = Integer.parseInt(strTokenizer.nextToken());
             }

             // max entries in OctTree
            String readingOut2ndLine = br.readLine();
            strTokenizer = new StringTokenizer(readingOut2ndLine);
            while (strTokenizer.hasMoreElements()){
                String tmp = strTokenizer.nextToken();
                if(tmp.equals("="))
                    MaximumEntriesinOctreeNode = Integer.parseInt(strTokenizer.nextToken());
            }


        }
        catch (FileNotFoundException fnfe){
             fnfe.printStackTrace();
             System.out.println("Configuration file cannot be found");
        }
        catch (IOException ioe){
            ioe.printStackTrace();
        }
    }
    public static void serialize(String path, Serializable obj){
        try {
            FileOutputStream fileOutStr = new FileOutputStream(path);
            ObjectOutputStream oos = new ObjectOutputStream(fileOutStr);
            oos.writeObject(obj);
            oos.close();
            fileOutStr.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }
    public static Object deserialize(String path) throws DBAppException{
        Object obj = null;
        try {
            FileInputStream fileInStr = new FileInputStream(path);
            ObjectInputStream ois = new ObjectInputStream(fileInStr);
            obj =ois.readObject();
            ois.close();
            fileInStr.close();
        } catch (IOException | ClassNotFoundException i) {
            i.printStackTrace();
            throw new DBAppException(path+" not found");
        }
        return obj;
    }


    //method 2 : insert
    public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException, ParseException {
        if(!listofCreatedTables.contains(strTableName))
            throw new DBAppException("You cannot insert into a table that has not been created yet");

        //1- fetch the table from the disk
        String path = "src/resources/tables/" + strTableName + "/" + strTableName + ".ser";
        Table tblToInsertInto = (Table) deserialize(path);

        //2- insert a record into it
        Vector<Object> vecValues = new Vector<>();
        Enumeration<String> strEnumeration = htblColNameValue.keys();
        while (strEnumeration.hasMoreElements()){
            String strColName = strEnumeration.nextElement();
            Column c = tblToInsertInto.getColumn(strColName);
            //if this Column does not exist , throw exception
            if(!tblToInsertInto.getVecColumns().contains(c))
                throw new DBAppException("No such column");
            //get the value
            Object strColValue = htblColNameValue.get(strColName);
            //check the value type
            try {
                tblToInsertInto.validateValueType(c,strColValue);

                // if it is the value of the clustering kry, insert into the first cell
                if(strColName.equals(tblToInsertInto.getStrClusteringKeyColumn())) // if(c.isPrimary())
                    vecValues.add(0,strColValue);
                else
                    vecValues.add(strColValue);
            }
            catch (DBAppException dbe){
                dbe.printStackTrace();
                throw new DBAppException(dbe.getMessage());
            }
        }
        Row entry = new Row(vecValues);
        tblToInsertInto.insertAnEntry(entry);
        //3-return table back to disk with the the new inserted value
        serialize(path,tblToInsertInto);

    }





    public void updateTable(String strTableName,String strClusteringKeyValue,Hashtable<String,Object> htblColNameValue )throws DBAppException{

    }
    public void deleteFromTable(String strTableName,Hashtable<String,Object> htblColNameValue)throws DBAppException{
      listofCreatedTables.remove(strTableName);
    }
   //    public static Iterator selectFromTable(SQLTerm[] arrSQLTerms,String[] strarrOperators)throws DBAppException{
  //  }

    public static void main(String[] args) throws DBAppException,InterruptedException,ParseException {

DBApp d = new DBApp();
Hashtable htNameType = new Hashtable();
htNameType.put("Id","java.lang.Integer");
htNameType.put("Name","java.lang.String");
htNameType.put("Job","java.lang.String");
Hashtable htNameMin = new Hashtable();
htNameMin.put("Id","1");
htNameMin.put("Name","AAA");
htNameMin.put("Job","blacksmith");
Hashtable htNameMax = new Hashtable();
htNameMax.put("Id","1000");
htNameMax.put("Name","zaky");
htNameMax.put("Job","zzz");

d.createTable("University","Id",htNameType,htNameMin,htNameMax);
Hashtable htColNameVal0 = new Hashtable();
htColNameVal0.put("Id" , new Integer(23) );
htColNameVal0.put("Name",new String("ahmed"));
htColNameVal0.put("Job" , new String("blacksmith"));

Hashtable htColNameVal1 = new Hashtable();
htColNameVal1.put("Id" , new Integer(33) );
htColNameVal1.put("Name",new String("ali"));
htColNameVal1.put("Job" , new String("engineer"));

Hashtable htColNameVal2 = new Hashtable();
htColNameVal2.put("Id" , new Integer(11) );
htColNameVal2.put("Name",new String("dani"));
htColNameVal2.put("Job" , new String("doctor"));


Hashtable htColNameVal3 = new Hashtable();
htColNameVal3.put("Id" , new Integer(15) );
        htColNameVal3.put("Job" , new String("teacher"));
htColNameVal3.put("Name",new String("basem"));


d.insertIntoTable("University",htColNameVal0);
d.insertIntoTable("University",htColNameVal1);
d.insertIntoTable("University",htColNameVal3);
d.insertIntoTable("University",htColNameVal2);

Table x = (Table)deserialize("src/resources/tables/University/University.ser");

System.out.println(x.toString());
        System.out.println("hello");

// 0,2,1,3
// 0,2,3,1
// 0,3,2,1
// 0,3,1,2
// 0,1,2,3
// 0,1,3,2


      /*
        Hashtable<String, String> htNameType = new Hashtable<>();
        htNameType.put("ahmed", "java.lang.String");
        Hashtable<String, String> htNameMin = new Hashtable<>();
        Hashtable<String, String> htNameMax = new Hashtable<>();
        htNameMin.put("ahmed", "2");
        htNameMax.put("ahmed", "7");



    try {
        validation(htNameType, htNameMin, htNameMax);
        System.out.println("right");
    }
    catch (DBAppException e){
        System.out.println(e.getMessage());
    }

*/

    }
}
//Iterator class
// Make a collection
        /*ArrayList<String> cars = new ArrayList<String>();
        cars.add("Volvo");
        cars.add("BMW");
        cars.add("Ford");
        cars.add("Mazda");

        // Get the iterator
        Iterator<String> it = cars.iterator();

        // Print the first item
        while(it.hasNext())
          System.out.println(it.next());

        */

        /*
            ArrayList<Integer> numbers = new ArrayList<Integer>();
    numbers.add(12);
    numbers.add(8);
    numbers.add(2);
    numbers.add(23);
    Iterator<Integer> it = numbers.iterator();
    while(it.hasNext()) {
      Integer i = it.next();
      if(i < 10) {
        it.remove();
      }
    }
    System.out.println(numbers);
  }
Trying to remove items using a for loop or a for-each loop would not work correctly
 because the collection is changing size at the same time that the code is trying to loop

         */