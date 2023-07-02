import java.io.*;
//import java.sql.Date;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.*;


//implements DBAppInterface{
public class DBApp implements Serializable{
	
	 Hashtable<String, Table> tables ;
	
	public DBApp(){
	
		init();
		
	}
	
	public void init(){
		
		File file = new File("./src/main/resources/Tables");

		if(!file.exists()){
			
			this.tables= new Hashtable<String, Table>();
			return;
		}
		
		try{
			
			FileInputStream fs=new FileInputStream("./src/main/resources/Tables");
			ObjectInputStream is=new ObjectInputStream(fs);//chaining in action
			@SuppressWarnings("unchecked")
			
			Hashtable<String, Table> tables= (Hashtable<String, Table>) is.readObject();	
			this.tables=tables;			
			is.close();
			

			
		}
		catch (Exception ex){
			ex.printStackTrace();
		}
				
		
	}

	public void saveTables(Hashtable<String, Table> tables){

			try{

				FileOutputStream fs=new FileOutputStream("./src/main/resources/Tables");
				ObjectOutputStream os=new ObjectOutputStream(fs);
				os.writeObject(tables);
				os.close();
			}

			catch (Exception ex){

				ex.printStackTrace();
			}
	}
	 
	public void createTable(String strTableName, String strClusteringKeyColumn, 
			Hashtable<String,String> htblColNameType, Hashtable<String,String> htblColNameMin, 
			Hashtable<String,String> htblColNameMax ) throws DBAppException {
		
		
		Set<String>keys = htblColNameType.keySet();
		String temp;
		
		
			if(tables.get(strTableName)!=null)
				throw new DBAppException("Table name already exists");
						
		try{
			
	    BufferedWriter csvWriter = new BufferedWriter(new FileWriter("./src/main/resources/metadata.csv", true));
	    
	    File file = new File("./src/main/resources/metadata.csv");
	    if(file.length()==0){
	    	
	    	temp="200";
	    	csvWriter.write(temp);
			csvWriter.newLine();	    	
	    }
		
		for(String key: keys){
			
			temp = strTableName+','+key+','+htblColNameType.get(key)+',';
			if(strClusteringKeyColumn.equals(key)) {
				temp = temp + true + ',';
			}
			else{
				temp = temp + false + ',';
				
			}
			temp = temp+ false+ ','+htblColNameMin.get(key)+','+htblColNameMax.get(key);//false in this line for index
				csvWriter.write(temp);
				csvWriter.newLine();
	        }
		csvWriter.close();
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
		
		Table newTable= new Table(strTableName,strClusteringKeyColumn,htblColNameType, getN());
		this.tables.put(strTableName, newTable);
		this.saveTables(this.tables);

		
		
	}
	
	@SuppressWarnings("resource")
	
	public static int getN()  {
		
	 BufferedReader csvReader;
	 try {
		csvReader = new BufferedReader(new FileReader("./src/main/resources/metadata.csv"));
		String row = csvReader.readLine();
		int N= Integer.parseInt(row);
		 return N;
	}catch (FileNotFoundException e) {

		e.printStackTrace();
	}
	catch (IOException e) {
		e.printStackTrace();
	}
	 
	return -1;
		
	}
	
		
	
	public boolean validateInput (String strTableName, Hashtable<String,Object> htblColNameValue )throws DBAppException{
		
		if(this.tables.get(strTableName)==null){
			System.out.println("table doesn't exist in database");
			throw new DBAppException("Table doesn't exist");

		}
		
		try{   	
			BufferedReader csvReader = new BufferedReader(new FileReader("./src/main/resources/metadata.csv"));
			Boolean doesKeyExist= false;
			boolean validInputSize=false;
			String row="";
			while ((row = csvReader.readLine()) != null) {
				
			    String[] data = row.split(",");
			       
			    if(data[0].equals(strTableName)){
			   	
			    	if(htblColNameValue.containsKey(data[1])){//check if line in csv file is relevant
			    		
			    		Object ob = htblColNameValue.get(data[1]);
			    		String type= ob.getClass().getName().toLowerCase();
			    		String inputType= data[2].toLowerCase();
			    		
			    		//check if type of input matches column type
			    		if(!type.equals(inputType)){
			    			System.out.println(data[1]+"  data input should match column type"+ data);
			    			csvReader.close();
			    			return false;
			    			//throw exception	
			    		} 
			    		
			    		//check if input in within accepted range of column data
			    		switch(type){
			    		
			    		case "java.lang.integer" :
			    			validInputSize=(Integer.parseInt(data[5]) <= (Integer)ob && Integer.parseInt(data[6])>(Integer)ob);
			    			break;
			    		case "java.lang.double" :
			    			validInputSize=(Double.parseDouble(data[5]) <= (Double)ob && Double.parseDouble(data[6])>(Double)ob);
			    			break;

			    		case "java.lang.string" :
			    			validInputSize=(((String)ob).compareTo(data[6])<= 0 && ((String)ob).compareTo(data[5])>0);
							System.out.println("I was here+ valid input size: "+validInputSize+ "String:"+ (String)ob);
			    			break;

			    		case "java.util.date":

							SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
							String date1 = data[5];
							Date dateMin =(Date) format.parse(date1);
							String date2 = data[6];
							Date dateMax = (Date) format.parse(date2);
							Date dateInput = (Date) ob;
							boolean d = ((dateInput).compareTo(dateMin)>=0 && (dateInput).compareTo(dateMax)<=0);
							break;
			    		    
			    		default: validInputSize=false;
	                        break;
			    		
			    		}
			    		// check it this hash table key is the tables primary key
			    		if(Boolean.parseBoolean(data[3])==true)
			    	         doesKeyExist=true;	  	    			
			        }	  	    	
			  }	
			    
		    }	
			//check that all columns user inputs are valid column names
			Set<String>tableNames = htblColNameValue.keySet();
			Hashtable<String, String> values=this.tables.get(strTableName).getcolNameType();
			for(String key: tableNames){
				if( values.get(key)==null){				
					System.out.println(key+" is an invalid column name");
					return false;
				}
		     }

			System.out.println("doesKeyExist: "+doesKeyExist);
			System.out.println("validInputSize: "+validInputSize);
			csvReader.close();
			if(doesKeyExist && validInputSize)
		    	return true;      	
            }
            
            catch(Exception ex){  	
            	ex.printStackTrace();
            }

		 return false;	
		
	}
	
    public void insertIntoTable(String strTableName,Hashtable<String,Object> htblColNameValue)throws DBAppException{

		if(!this.validateInput(strTableName, htblColNameValue)){
			 throw new DBAppException("Invalid input");
		}

		Table table=this.tables.get(strTableName);
		table.insertTuple(htblColNameValue);
		this.saveTables(tables);
							
	}
	
    public void updateTable(String strTableName,  String strClusteringKeyValue, 
			Hashtable<String,Object> htblColNameValue ) throws DBAppException ,ParseException{

		Table table=this.tables.get(strTableName);
		Hashtable<String,Object> temp = htblColNameValue;
		if(table!=null){
			switch(table.colNameType.get(table.primaryKey) ){
				case "java.lang.Integer":temp.put(table.primaryKey, Integer.parseInt(strClusteringKeyValue)); break;
				case "java.lang.String":temp.put(table.primaryKey, strClusteringKeyValue);break;
				case "java.lang.Double":temp.put(table.primaryKey, Double.parseDouble(strClusteringKeyValue));break;
				case "java.util.Date":
					SimpleDateFormat d = new SimpleDateFormat("yyyy-MM-dd");
					Date date1 =(Date) d.parse(strClusteringKeyValue);
					temp.put(table.primaryKey,date1);break;
			}

			if(!this.validateInput(strTableName, temp)){
				throw new DBAppException("invalid input");
			}
			table.updateTuple(strClusteringKeyValue,htblColNameValue);
			this.saveTables(tables);
		}

		
	}

	public void deleteFromTable(String strTableName,Hashtable<String,Object> htblColNameValue) throws DBAppException{
		
		Table table=this.tables.get(strTableName);
		if(table!=null){
		table.deleteTuples(htblColNameValue);
		this.saveTables(tables);}
		else
		 throw new DBAppException("Table doesn't exist");
				
	}

	public void createIndex(String strTableName, String[] strarrColName) throws DBAppException, ParseException{
		//check for dublicate columns
		//could create several indexes for the same table
		// if i have a grid (age-salary-department), this method was invoked for (age-salary), just use old index and ignore dimension

		if(this.tables.get(strTableName)==null){
			System.out.println("table doesn't exist in database");
			throw new DBAppException("Table doesn't exist");

		}
		Table table=this.tables.get(strTableName);
		String [][] ranges= this.readRanges(strTableName,strarrColName);
		tables.get(strTableName).indexes.add(new Grid(ranges,table));
		this.saveTables(this.tables);

	}

	public String [][] readRanges(String strTableName, String[] strarrColName){
        int i=0;
		String [][] ranges= new String[strarrColName.length][4] ;
		try {

			BufferedReader csvReader = new BufferedReader(new FileReader("./src/main/resources/metadata.csv"));
			String row = "";

			while ((row = csvReader.readLine()) != null) {

				String[] data = row.split(",");

				if (data[0].equals(strTableName)) {

					if (Arrays.asList(strarrColName).contains(data[1])) {//check if line in csv file is relevant

						ranges[i][0] = data[1];
						ranges[i][1] = data[5];
						ranges[i][2] = data[6];
						ranges[i][3] = data[2];
						i++;
						if (i == strarrColName.length)
							return ranges;
					}
				}

			}

		}


		catch(Exception ex){
			ex.printStackTrace();
		}
		return null;
	}

	public Iterator selectFromTable( SQLTerm [] arrSQLTerms, String[] strarrOperators) throws DBAppException{

		Table table=tables.get(arrSQLTerms[0]._strTableName);
		Vector <Object> infix=new Vector<Object>();



		for(int i=0; i<arrSQLTerms.length;i++){

			infix.add(arrSQLTerms[i]);
			if(strarrOperators.length-1>i)
			infix.add(strarrOperators[i]);
		}
		infix.add(arrSQLTerms[arrSQLTerms.length-1]);
		Vector<Object> postfix= this.postfix(infix);

		Iterator result= this.evaluatePostfix(postfix,table);

        return result;

	}

	public int precendence (String str){

		   switch (str)
			{
				case "AND":
					return 3;
				case "OR":
					return 2;
				case "XOR":
					return 1;
			}
			return -1;

	}

	public Vector<Object> postfix(Vector<Object>infix){

		Vector<Object>result=new Vector<Object>();
		// initializing empty stack
		Stack<String> stack = new Stack<String>();

		for (int i = 0; i< infix.size(); ++i)
		{
			Object current=infix.get(i);
			// If the scanned Object is an
			// operand, add it to output.
			if (current instanceof SQLTerm)
				result.add(current);

			else // an operator is encountered
			{
				while (!stack.isEmpty() && precendence((String)current)
						< precendence (stack.peek())) { /////
					result.add(stack.pop());
				}
				stack.push((String)current);
			}

		}
		// pop all the operators from the stack
		while (!stack.isEmpty()){
			result.add(stack.pop());
		}
		return result;
	}

	public Iterator evaluatePostfix(Vector<Object> exp, Table table) throws DBAppException
	{
		System.out.println("size is"+exp.size());
		if(exp.size()==2){
			System.out.println("calling select with one operator");
			return  table.selectWithoutIndex(exp,"AND").iterator();
		}
		//create a stack
		Stack<Object> stack=new Stack<>();
		// Scan all characters one by one
		for(int i=0;i<exp.size();i++)
		{
			Object current= exp.get(i);
			// If the scanned Object is an operand (number here),
			// push it to the stack.
			if(!current.getClass().getName().toLowerCase().equals("java.lang.string"))

				stack.push(current);

				//  If the scanned character is an operator, pop two
				// elements from stack apply the operator
			else
			{
				Vector<Object> operands=new Vector<Object>();
				operands.add(stack.pop());
				operands.add(stack.pop());
				Object next =exp.get(i+1);
				while(current.equals(next)) {
					operands.add(stack.pop());
					next=exp.get((++i)+1);
				}

				switch((String)current)
				{
					case "XOR":
						stack.push(table.XorOperands(operands));
						break;

					case "OR":
						stack.push(table.OrOperands(operands));
						break;

					case "AND":
						stack.push(table.AndOperands(operands));
						break;


				}
			}
		}
		return (Iterator)stack.pop();
	}

	public static void main(String []args) throws DBAppException, ParseException{
		


        String strTableName = "Student";
		DBApp dbApp = new DBApp( );
		Hashtable htblColNameType = new Hashtable( );
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");

		Hashtable htblColMin = new Hashtable( );
		htblColMin.put("id","0");
		htblColMin.put("name",new String(  "AAAAAA"));
		htblColMin.put("gpa",new String(  "0.7"));

		Hashtable htblColMax = new Hashtable( );
		htblColMax.put("id","1000");
		htblColMax.put("name",new String(  "zzzzzz"));
		htblColMax.put("gpa",new String(  "5.0"));



		dbApp.createTable( strTableName, "id", htblColNameType, htblColMin, htblColMax);

         dbApp.createIndex( strTableName, new String[] {"gpa","id"} );
         Hashtable htblColNameValue = new Hashtable( );
         htblColNameValue.put("id",10);
         htblColNameValue.put("name", new String("Ahmedd" ) );
         htblColNameValue.put("gpa", new Double( 0.90 ) );
         dbApp.insertIntoTable( strTableName , htblColNameValue );


		Hashtable h2 = new Hashtable( );
         h2.put("id",14 );
         h2.put("name", new String("Noorrr" ) );
         h2.put("gpa", new Double( 0.95 ) );
         dbApp.insertIntoTable( strTableName , h2 );

		Hashtable h3 = new Hashtable( );
       h3.put("id", 24);
       h3.put("name", new String("Daliaa" ) );
       h3.put("gpa", new Double( 1.3 ) );
       dbApp.insertIntoTable( strTableName , h3 );


		Hashtable h4 = new Hashtable( );
       h4.put("id",400);
       h4.put("name", new String("Johnnn" ) );
       h4.put("gpa", new Double( 1.1 ) );
       dbApp.insertIntoTable( strTableName , h4);

		Hashtable h5 = new Hashtable( );
       h5.put("id", 600);
       h5.put("name", new String("Zakyyy" ) );
       h5.put("gpa", new Double( 3.0) );
       ////////compare to in linearSearch gave null pionter exception when gpa wasn't put in this tuple
       dbApp.insertIntoTable( strTableName , h5);

        Table table=dbApp.tables.get("Student");
		Vector<Object>test = new Vector<Object>();
		SQLTerm[] arrSQLTerms;
		arrSQLTerms = new SQLTerm[1];
		arrSQLTerms[0]=new SQLTerm();
		arrSQLTerms[0]._strTableName = "Student";
		arrSQLTerms[0]._strColumnName= "gpa";
		arrSQLTerms[0]._strOperator = "<=";
		arrSQLTerms[0]._objValue = 0.95;

		test.add(arrSQLTerms[0]);

        String [] arr={};
		Iterator result= dbApp.selectFromTable(arrSQLTerms,arr);
		table.indexes.get(0).displayGrid(table.indexes.get(0).gridIndex);
		System.out.println("displaying result for (select from student where gpa=0.95)");
		while (result.hasNext())
			System.out.println(result.next());






	}


     




}
	
	
	
	
	

