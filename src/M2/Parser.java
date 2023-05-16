package M2;

import M1.*;
import M1.DBAppException;
import M1.Table.Tuple3;

import java.io.*;

import java.util.Iterator;
import java.util.*;
import M1.DBApp;

public class Parser {

	public Parser() throws DBAppException {
		Scanner sc = new Scanner(System.in);
		System.out.println(" you must end every sql statement with space before pressing enter"
				+ "\n\n ----EXAMPLES OF SQL STATEMENTS FOR TESTING"
				+ "\ncreate table employee(id int check(id between 1 and 1000),name varchar check (name between 'a' and 'zzzzzzzz'),salary double check(salary between 0 and 10000),dob date check(dob between '1930/01/01' and '2030/12/31'),primary key(id))"
				+ "\ninsert into employee values(103,'akrm',1000,'1990/10/10);"
				+ "\ndelete from employee where id = 103 ;" + "\nupdate employee set name = 'akram' where id = 103 ;"
				+ "\ndrop table employee ;" + "\ndrop index nameAgeIDIndex on Employee ;");

		System.out.println("Enter your SQL Statements HERE & to end console session type 'exit' or 'end':::::\n\n");

		while (true) {
			System.out.println("Enter statement: ");
			String instruction = sc.nextLine();
			if (instruction.equalsIgnoreCase("exit") || instruction.equalsIgnoreCase("end")) {
				System.out.println("\n\nEXITED Program! Thank You for trying");
				break;
			}
			// CheckerOfString sa = new CheckerOfString(instruction);

			Iterator res = this.parseSQL(instruction);
			while (res != null && res.hasNext()) {
				System.out.println(res.next());
			}

		}
		DBApp d = new DBApp();
		d.toString();
	}

	public Iterator parseSQL(String s) throws DBAppException {
		return parseSQL(new StringBuffer(s));
	}

	public Iterator parseSQL(StringBuffer strbufSQL) throws DBAppException {
		CheckerOfString checker = new CheckerOfString(strbufSQL.toString());
		if (checker.hasMoreWords()) {
			String command = checker.nextWord();
			if (command.equals("create")) {
				String type = checker.nextWord();
				if (type.equals("table")) {
					createTable(checker);
					return null;
				} else if (type.equals("index")) {
					createIndex(checker);
					return null;
				} else {
					throw new DBAppException("undefined create type. you asked to create a " + type);
				}
			} else if (command.equals("insert")) {
				insert(checker);
				return null;
			} else if (command.equals("update")) {
				update(checker);
				return null;
			} else if (command.equals("delete")) {
				delete(checker);
				return null;
			} else if (command.equals("select")) {
				return select(checker);
			} else if (command.equals("drop")) {
				drop(checker);
				return null;
			} else if (command.equals("show")) {
				show(checker);
				return null;
			} else {
				throw new DBAppException("undefined instruction " + command);
			}
		} else
			return null;
	}

	public Iterator select(CheckerOfString checker) throws DBAppException {
		if (!checker.nextWord().equals("*"))
			throw new DBAppException("you have to add * after select, for we don't support projection");
		if (!checker.nextWord().equals("from"))
			throw new DBAppException("you have to add FROM to specify the selection table");
		String tableName = checker.nextWord();
		String path = "src/resources/tables/" + tableName + "/" + tableName + ".ser";
		Table table = (Table) DBApp.deserialize(path);
		Vector<String> operators = new Vector<String>();
		Vector<SQLTerm> terms = new Vector<SQLTerm>();
		if (!checker.hasMoreWords() || !checker.nextWord().equals("where"))
			throw new DBAppException("you have to add WHERE to specify the selection terms");
		while (checker.hasMoreWords()) {
			String columnName = checker.nextWord();
			String operator = checker.nextWord();
			String value = checker.nextWord();
			String type = table.getColumn(columnName).getStrColType();
			if (type.equals("java.lang.String") || type.equals("java.util.Date"))
				if (value.charAt(0) != '\'' || value.charAt(value.length() - 1) != '\'')
					throw new DBAppException("put varchar and date in single quotes");
				else
					value = value.substring(1, value.length() - 1);
			Object val = Methods2.parseType(value, type);
			SQLTerm t = new SQLTerm(tableName, columnName, operator, val);
			terms.add(t);
			if (checker.hasMoreWords())
				operators.add(checker.nextWord());
		}
		DBApp db = new DBApp();
		String[] arrayOperators = new String[operators.size()];
		for (int i = 0; i < arrayOperators.length; i++)
			arrayOperators[i] = operators.get(i).toUpperCase();
		SQLTerm[] sqlTerms = new SQLTerm[terms.size()];
		for (int i = 0; i < sqlTerms.length; i++)
			sqlTerms[i] = terms.get(i);
//		System.out.println(Arrays.toString(sqlTerms));
//		System.out.println(Arrays.toString(arrayOperators));
		return db.selectFromTable(sqlTerms, arrayOperators);
	}

	public void delete(CheckerOfString checker) throws DBAppException {
		if (!checker.nextWord().equals("from"))
			throw new DBAppException("you have to add FROM to specify the deletion table");
		String tableName = checker.nextWord();
		String path = "src/resources/tables/" + tableName + "/" + tableName + ".ser";
		Table table = (Table) DBApp.deserialize(path);
		Hashtable<String, Object> columnNameValue = new Hashtable<String, Object>();
		if (checker.hasMoreWords() && !checker.readNextWord().equals("where"))
			throw new DBAppException("you have to add WHERE to specify the ANDED Deletion conitions");
		else if (checker.hasMoreWords() && checker.nextWord().equals("where")) {
			while (checker.hasMoreWords()) {
				String columnName = checker.nextWord();
				if (!checker.hasMoreWords() || !checker.nextWord().equals("=")) {
					// System.out.println(checker.readNextWord());
					throw new DBAppException("expected = operator in deletion conditions");
				}
				String value = checker.nextWord();
				Column c = table.getColumn(columnName);
				if (c == null) {
					throw new DBAppException("Invalid column name");
				}
				String type = c.getStrColType();
				if (type.equals("java.lang.String") || type.equals("java.util.Date"))
					if (value.charAt(0) != '\'' || value.charAt(value.length() - 1) != '\'')
						throw new DBAppException("put varchar and date in single quotes");
					else
						value = value.substring(1, value.length() - 1);
				Object val = Methods2.parseType(value, type);
				columnNameValue.put(columnName, val);
				if (checker.hasMoreWords() && !checker.nextWord().equals("and"))
					throw new DBAppException("expected AND operator in deletion condiitons");
			}
		} else if (checker.hasMoreWords())
			throw new DBAppException("syntax error: unexpected " + checker.nextWord());
		DBApp db = new DBApp();
		db.deleteFromTable(tableName, columnNameValue);
	}

	public void update(CheckerOfString checker) throws DBAppException {
		String tableName = checker.nextWord();
		String path = "src/resources/tables/" + tableName + "/" + tableName + ".ser";
		Table table = (Table) DBApp.deserialize(path);
		Hashtable<String, Object> columnNameValue = new Hashtable<String, Object>();
		if (!checker.nextWord().equals("set"))
			throw new DBAppException("you have to add SET to specify the update columns");
		while (checker.hasMoreWords()) {
			String columnName = checker.nextWord();
			if (!checker.nextWord().equals("="))
				throw new DBAppException("expected = after column name in updated columns");
			String value = checker.nextWord();
			Column c = table.getColumn(columnName);
			String type = c.getStrColType();
			if (type.equals("java.lang.String") || type.equals("java.util.Date"))
				if (value.charAt(0) != '\'' || value.charAt(value.length() - 1) != '\'')
					throw new DBAppException("put varchar and date in single quotes");
				else
					value = value.substring(1, value.length() - 1);
			Object val = Methods2.parseType(value, type);
			columnNameValue.put(columnName, val);
			if (checker.readNextWord().equals("where"))
				break;
			if (!checker.nextWord().equals(","))
				throw new DBAppException("expected , between updated columns");
		}
		if (!checker.nextWord().equals("where"))
			throw new DBAppException("expected where after updated columns");
		String clusteringKey = checker.nextWord();
		if (!clusteringKey.equals(table.getStrClusteringKeyColumn()))
			throw new DBAppException("update condition should be only on clustering key");
		if (!checker.nextWord().equals("="))
			throw new DBAppException("expected = after clustering key");
		String clusteringKeyValue = checker.nextWord();
		if (checker.hasMoreWords())
			throw new DBAppException("syntax error: unexpected " + checker.nextWord());
		DBApp db = new DBApp();
		db.updateTable(tableName, clusteringKeyValue, columnNameValue);
	}

	public void insert(CheckerOfString checker) throws DBAppException {
		Hashtable<String, Object> colNameValue = new Hashtable<String, Object>();
		if (checker.nextWord().equals("into")) {
			String tableName = checker.nextWord();
			String path = "src/resources/tables/" + tableName + "/" + tableName + ".ser";
			Table table = (Table) DBApp.deserialize(path);
			Vector<Column> columns = table.getVecColumns();
			if (checker.nextWord().equals("values") && checker.nextWord().equals("(")) {
				for (int i = 0; i < columns.size(); i++) {
					String type = columns.get(i).getStrColType();
					String value = checker.nextWord();
					if (type.equals("java.lang.String") || type.equals("java.util.Date"))
						if (value.charAt(0) != '\'' || value.charAt(value.length() - 1) != '\'')
							throw new DBAppException("put varchar and date in single quotes");
						else
							value = value.substring(1, value.length() - 1);

					Object val;
					try {

						val = Methods2.parseType(value, type);
					} catch (Exception e) {
						System.err.print("Check table definition:\n " + tableName + " ===> ");
						for (Column column : columns) {
							System.err.print(column.getStrColName() + "::" + column.getStrColType() + " , ");
						}
						System.err.println("\n Please retry with same order!\n");
						throw e;
					}
					colNameValue.put(columns.get(i).getStrColName(), val);
					if (checker.readNextWord().equals(")"))
						break;
					if (!checker.nextWord().equals(","))
						throw new DBAppException(
								"separate columns with commas && make sure to insert values for all columns");
				}
				if (!checker.nextWord().equals(")"))
					throw new DBAppException("end the parameters with )");
				DBApp db = new DBApp();
				db.insertIntoTable(tableName, colNameValue);
			} else {
				throw new DBAppException("Syntax error, expected command..Values followed by (");
			}

		} else {
			throw new DBAppException("Syntax error, expected command..INTO");
		}

	}

	public void createIndex(CheckerOfString checker) throws DBAppException {
		checker.nextWord();
		if (!checker.nextWord().equals("on"))
			throw new DBAppException("syntax error: type on after index name");
		String tableName = checker.nextWord();
		Vector<String> columns = new Vector<String>();
		if (!checker.nextWord().equals("("))
			throw new DBAppException("put indexed columns in parantheses");
		while (true) {
			columns.add(checker.nextWord());
			if (checker.readNextWord().equals(")"))
				break;
			if (!checker.nextWord().equals(","))
				throw new DBAppException("separate indexed columns with commas");
		}
		checker.nextWord();
		String[] columnNames = new String[columns.size()];
		for (int i = 0; i < columnNames.length; i++)
			columnNames[i] = columns.get(i);
		DBApp db = new DBApp();
		db.createIndex(tableName, columnNames);

	}

	public void createTable(CheckerOfString checker) throws DBAppException {
		String tableName = checker.nextWord();
		String clusteringKey = "";
		Hashtable<String, String> colNameType = new Hashtable<String, String>();
		Hashtable<String, String> colNameMin = new Hashtable<String, String>();
		Hashtable<String, String> colNameMax = new Hashtable<String, String>();
		if (!checker.nextWord().equals("("))
			throw new DBAppException("put table columns in parantheses");
		while (!checker.readNextWord().equals(")")) {
			String colName = checker.nextWord();
			if (colName.equals("primary")) {
				if (checker.nextWord().equals("key")) {
					if (!checker.nextWord().equals("("))
						throw new DBAppException("put primary key in parantheses");
					clusteringKey = checker.nextWord();
					if (!checker.nextWord().equals(")"))
						throw new DBAppException("put primary key in parantheses");
				} else
					throw new DBAppException("syntax error: type key after primary to specify primary key");
				if (checker.readNextWord().equals(")"))
					break;
				if (!checker.nextWord().equals(","))
					throw new DBAppException("you must separate input columns with commas");
				continue;
			}
			String colType = checker.nextWord();
			// System.out.println(checker.readNextWord());
			if (!checker.nextWord().equals("check") || !checker.nextWord().equals("(")
					|| !checker.nextWord().equals(colName) || !checker.nextWord().equals("between")) {
				throw new DBAppException(
						"Add check constraint similar to CHECK(COLUMN_NAME BETWEEN MIN_VALUE AND MAX_VALUE) to specify each column min and max values");
			}
			String colMin = checker.nextWord();
			if (colType.equals("varchar") || colType.equals("date"))
				if (colMin.charAt(0) != '\'' || colMin.charAt(colMin.length() - 1) != '\'')
					throw new DBAppException("put varchar and date in single quotes for min value");
				else
					colMin = colMin.substring(1, colMin.length() - 1);
			if (!checker.nextWord().equals("and")) {
				throw new DBAppException(
						"Add check constraint similar to CHECK(COLUMN_NAME BETWEEN MIN_VALUE AND MAX_VALUE) to specify each column min and max values");
			}
			String colMax = checker.nextWord();
			if (colType.equals("varchar") || colType.equals("date"))
				if (colMax.charAt(0) != '\'' || colMax.charAt(colMax.length() - 1) != '\'')
					throw new DBAppException("put varchar and date in single quotes for max value");
				else
					colMax = colMax.substring(1, colMax.length() - 1);
			if (!checker.nextWord().equals(")")) {
				throw new DBAppException(
						"Add check constraint similar to CHECK(COLUMN_NAME BETWEEN MIN_VALUE AND MAX_VALUE) to specify each column min and max values");
			}
			switch (colType) {
			case "int":
				colType = "java.lang.Integer";
				break;
			case "double":
				colType = "java.lang.Double";
				break;
			case "date":
				colType = "java.util.Date";
				break;
			case "varchar":
				colType = "java.lang.String";
				break;
			default:
				throw new DBAppException("Syntax error, unsupported data type " + colType);
			}
			colNameType.put(colName, colType);
			colNameMin.put(colName, colMin);
			colNameMax.put(colName, colMax);
			if (checker.readNextWord().equals(")"))
				break;
			if (!checker.nextWord().equals(","))
				throw new DBAppException("separate input columns with commas");
		}
		checker.nextWord();
		if (colNameType.size() == 0)
			throw new DBAppException("table must have at least one column");
		if (clusteringKey.equals(""))
			throw new DBAppException(
					"table must have a primary (clustering) key. Specify using: PRIMARY KEY(INTENDED_KEY)");
		DBApp db = new DBApp();
		// listofCreatedTables.add(tableName);
		db.createTable(tableName, clusteringKey, colNameType, colNameMin, colNameMax);
	}

	public static void deleteDirectory(File file) {
		File[] contents = file.listFiles();
		if (contents != null) {
			for (File f : contents) {
				deleteDirectory(f);
			}
		}
		file.delete();
	}

	private static void removeIndexOnColsFromCSV(String strTableName, String indxName) throws DBAppException {
		// first, read all data and extract all tables other than the given table
		List<String> data = new ArrayList<>();
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader("MetaData.csv"));
			String line = br.readLine();

			while (line != null) {
				String[] attributes = line.split(",");
				if (attributes[0].equals(strTableName) && attributes[4].equals(indxName)) {
					line = "";
					for (int i = 0; i < attributes.length; i++) {
						if (i != 4)
							line += attributes[i] + ((i < 6) ? "," : "");
						else
							line += "null,";
					}

				}
				data.add(line);

				line = br.readLine();
			}
			br.close();
		} catch (IOException e) {
			throw new DBAppException("Error reading csv file");
		}

		// second, write the new data to the csv file
		try {
			File file = new File("MetaData.csv");
			new FileWriter(file, false).close();
			;
			FileWriter fr = new FileWriter(file, true);
			PrintWriter printWriter = new PrintWriter(fr);
			StringBuilder stringBuilder = new StringBuilder();
			for (String line : data) {
				stringBuilder.append(line);
				stringBuilder.append('\n');
			}
			printWriter.write(stringBuilder.toString());
			printWriter.flush();
			printWriter.close();
		} catch (IOException e) {
			throw new DBAppException(e.getMessage());
		}

	}

	public static void drop(CheckerOfString sa) throws DBAppException {
		DBApp dbApp = new DBApp();

		if (sa.readNextWord().equals("table")) {
			sa.nextWord();
			String tableName = sa.nextWord();
			dbApp.DELETETableDependencies(tableName);
			String path = "src/resources/tables/" + tableName;
			File index = new File(path);
			deleteDirectory(index);
			System.out.println("the table " + tableName + " has been deleted");
		} else if (sa.nextWord().equals("index")) {
			String indx = sa.nextWord();
			sa.nextWord();
			// sa.nextWord();
			String tableName = sa.nextWord();
			String path = "src/resources/tables/" + tableName + "/Indicies/" + indx + ".ser";
			File f = new File(path);
			deleteDirectory(f);
			path = "src/resources/tables/" + tableName + "/" + tableName + ".ser";
			Table t = (Table) DBApp.deserialize(path);

			Iterator<Tuple3> itr = t.getIndices().iterator();
			while (itr.hasNext()) { // remove index from Vector of table indicies
				Table.Tuple3 tuple = (Table.Tuple3) itr.next();

				if (tuple.getFilename().equals(indx + ".ser")) {
					itr.remove();
					break;
				}
			}

			// remove index from csv and replace index field
			removeIndexOnColsFromCSV(tableName, indx);

			DBApp.serialize(path, t);
			System.out.println("the index " + indx + " on table " + tableName + " have been deleted");
		}
	}

	public static void show(CheckerOfString sa) throws DBAppException {
		try {
			if (sa.readNextWord().equals("show")) {
				sa.nextWord();
				if (sa.readNextWord().equals("table")) {
					sa.nextWord();
					String tableName = sa.nextWord();
					String path = "src/resources/tables/" + tableName + "/" + tableName + ".ser";
					Table t = (Table) DBApp.deserialize(path);
					System.out.println(t);
				} else if (sa.nextWord().equals("index")) {
					int id = Integer.parseInt(sa.nextWord());
					sa.nextWord();
					sa.nextWord();
					String tableName = sa.nextWord();
					String path = "src/resources/tables/" + tableName + "/" + tableName + ".ser";
					Table t = (Table) DBApp.deserialize(path);
					t.getIndices().get(id).getOctree(tableName).printOctree();
					;
				}
			}
		} catch (NumberFormatException e) {
			throw new DBAppException("Input error" + e.getMessage());
		}
	}

	public static void STARTCODE() throws DBAppException {
		Parser p = new Parser();

	}

	public static void main(String[] args) throws DBAppException {
		// you must end every sql statement with space before pressing enter AND to END
		// type exit or end

		// EXAMPLE FOR TESTING
		// create table employee(id int check(id between 1 and 1000),name varchar check
		// (name between 'a' and 'zzzzzzzz'),salary double check(salary between 0 and
		// 10000),dob date check(dob between '1930/01/01' and '2030/12/31'),primary
		// key(id))
		// insert into employee values(103,'akrm',1000,'1990/10/10);
		// delete from employee where id = 103 ;
		// update employee set name = 'akram' where id = 103 ;

		STARTCODE();

	}
}