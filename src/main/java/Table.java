import java.io.*;
//import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/*

1- Method to insert into index new tuples
2- Method to populate grid index if table was already populated before the grid was created
3- Method to locate tuple in a grid => update/delete
4- Method to delete using index + add index condition in old delete function
5-Method to update using index + add index condition in old delete function

*/


public class Table implements Serializable{
	
	 public String name;
	 public String primaryKey;
	 public Hashtable<String, String> colNameType;
	 public Hashtable<String, Page > pages;
	 Vector<Grid>indexes;
	 int noOfPages;
	 int N;
	 
	 
	 
	 public Table(String name, String key, Hashtable <String, String> columns, int N){
		 
		 this.name=name;
		 this.primaryKey=key;
		 this.colNameType=columns;
		 this.noOfPages=0;
		 this.pages=new Hashtable <String,Page>();
		 this.N=N;
		 this.createNewPage(N);
		 this.indexes=new Vector<Grid>();
		 	 
	 }
	 
	 public Hashtable<String, String> getcolNameType(){
		 
		 return this.colNameType;
	 }

	 public Page createNewPage (int N){
		 
		 Page newPage= new Page(this.name,noOfPages,this.N);
		 String filename=this.name+this.noOfPages;
		 this.noOfPages++;
		 pages.put(filename,newPage);
		 return newPage;
	 }

	 
	 public static int compareTo(Object a, Object b) throws DBAppException {
			String type= a.getClass().getName().toLowerCase();
			
			if (!type.equals( b.getClass().getName().toLowerCase()))
				throw new DBAppException("The 2 objects are of different data types");
			
			else {

				switch (type) {

					case "java.lang.string":
						return ((String) a).compareTo((String) b);

					case "java.lang.integer":
						return ((Integer) a).compareTo((Integer) b);

					case "java.lang.double":
						return ((Double) a).compareTo((Double) b);

					case "java.util.date":
						return ((Date)a).compareTo((Date) b);

					default:
						throw new DBAppException("Invalid Data Type");

				}
			}
	 }

	 public int compareStrings(String s){
		 int uniCodeSum=0;
		 for(int i=0;i<s.length()-1;i++)
			 uniCodeSum+=s.codePointAt(i);
		 return uniCodeSum;
	 }

	public Page getPage(Hashtable<String,Object> htblColNameValue) throws DBAppException
    {
        int left = 0, right = (this.noOfPages-1);
 
        String midPageFileName;
        Page retrievedPage;
        Object key =  htblColNameValue.get(this.primaryKey);

        while (true) {

        	if(left>right){
             	if(right==-1) right=0; //if inserted element is less than any value in out table, insert in fist page
             	return this.pages.get(this.name+right);}
        	
        	
            int middle = left + (right - 1) / 2;
            midPageFileName = this.name + middle;
			retrievedPage= this.pages.get(midPageFileName);

            // Check if key is present in the range of the retrieved(middle) page
			if (compareTo(retrievedPage.min,key) <= 0 && compareTo(retrievedPage.max, key) >=0){
	
                return retrievedPage;}
                       
            // If key is greater than retrieved pages maximum, update left pointer
            if (compareTo(retrievedPage.max ,key) <0)
                 left = middle + 1;

            // If key is smaller than retrieved page's minimum, update right pointer
            else
                right = middle - 1;
                                           
			}
             
    }
	public Page getPageUpdate(Object key) throws DBAppException
	 {
     int left = 0, right = (this.noOfPages-1);

     String midPageFileName;
     Page retrievedPage;
     
     while (true) {
     	
     	if(left>right){
     		throw new DBAppException("page not found");
          	}
     	
     	
         int middle = left + (right - 1) / 2;

         midPageFileName = this.name + middle;
			retrievedPage= this.pages.get(midPageFileName);
			

         // Check if key is present in the range of the retrieved(middle) page
         if (compareTo(retrievedPage.min,key) <= 0 && compareTo(retrievedPage.max, key) >=0)//compare
             return retrievedPage;
             
                    
         // If key is greater than retrieved pages maximum, update left pointer
         if (compareTo(retrievedPage.max ,key) <0)//compare
              left = middle + 1;
                                                       
       
         // If key is smaller than retrieved page's minimum, update right pointer
         else
             right = middle - 1;
                                        
       }
          
  }

	public void insertTuple(Hashtable<String,Object> htblColNameValue) throws DBAppException {
		
		String filename;
		Page nextPage=null;
		Page p;
		
		if(pages.size()>1)
		   p=this.getPage(htblColNameValue);
		else
		   p=this.pages.get(this.name+0);

		int index=(int)p.getIndexInsert(this.primaryKey,htblColNameValue);
		Hashtable<String,Object> overflow= p.insertIntoPage(htblColNameValue, index, this.primaryKey);
		
				
	    if(overflow!=null){

	    	if(overflow.get(this.primaryKey).equals(htblColNameValue.get(this.primaryKey)))
	    	
	    	if((p.pageNumber+1)<noOfPages){
	 
	    		 filename=this.name+(p.pageNumber+1);
			     nextPage=this.pages.get(filename);
			     
			     if(nextPage.getNoOfRecords()==nextPage.MaximumCapacity){
			    	 
			     
			    	 nextPage=this.createNewPage(this.N);
			    	 nextPage.setPageNumber(p.pageNumber+1);
			    	 nextPage.insertIntoPage(overflow, 0, this.primaryKey);
			         this.updatePagesInsert(nextPage);
			    	 
			     }
			     else{
			    	
			       nextPage.insertIntoPage(overflow, 0, this.primaryKey); 
			       nextPage.setMin(overflow.get(this.primaryKey));
			      
			     }
			    	 
	    	}
	    		
	    	else{
	    		nextPage=this.createNewPage(this.N);
	    		nextPage.insertIntoPage(overflow, 0, this.primaryKey);

	    	}

	    	if(this.indexes.size()!=0 && overflow.get(this.primaryKey).equals(htblColNameValue.get(this.primaryKey))){
				this.insertIntoGrids(htblColNameValue,nextPage.getName());
	    	}


	    	else if(this.indexes.size()!=0){
				System.out.println("second if");
				this.insertIntoGrids(htblColNameValue,p.getName());
				////////////////////delete overflow pionter from index
				this.insertIntoGrids(htblColNameValue,nextPage.getName());
			}

	    }
	    else {

			if(this.indexes.size()!=0){

				System.out.println("third if");
				this.insertIntoGrids(htblColNameValue, p.getName());
			}
		}

	}
	
	public void updatePagesInsert(Page page){
		
		int pageNumber=page.getPageNumber();
		
		String last=this.name+this.pages.size();
    
    	for(int i = pageNumber ; i <= this.pages.size()-1 ;i++) {
    		
    		page=this.pages.replace(this.name+i,page); 
    		page.setPageNumber(i+1);
    	
    	}

		
		
	}
	
	public void deleteTuples(Hashtable<String,Object> htblColNameValue) throws DBAppException{
		
		
		if(htblColNameValue.get(this.primaryKey)!=null){
			deleteTuple(htblColNameValue);
			return;
		}

		   Page current;
		   Vector<Hashtable<String,Object>> output= new Vector<Hashtable<String,Object>> ();
		   Vector<Hashtable<String,Object>> temp=null;
		   Set<String> keys= this.pages.keySet();
		   
			for(String key: keys){
				
				current =pages.get(key);
				temp=current.linearSearchPage(htblColNameValue);
				if(temp!=null)
				output.addAll(temp);
								
		    }	
			for(int i =0; i< output.size();i++)
				this.deleteTuple(output.get(i));
			
			
	}
	
	public void deleteTuple(Hashtable<String,Object> htblColNameValue) throws DBAppException {
		
		Page target = this.getPage(htblColNameValue);
		
		int indexoftuple =(int) target.getIndexDelete(this.primaryKey, htblColNameValue);
		Vector <Hashtable<String,Object>>loadedPage =target.loadPage();
		
		
		if(indexoftuple==0 && loadedPage.size()>1){
			Hashtable<String, Object> y = (Hashtable<String, Object>)loadedPage.get(1) ;
			target.setMin(y.get(this.primaryKey));				
		}
		if(indexoftuple==loadedPage.size()-1 && loadedPage.size()>1){
			Hashtable<String, Object> y = (Hashtable<String, Object>)loadedPage.get(loadedPage.size()-2) ;
			target.setMax(y.get(this.primaryKey));				
	    }
		
		loadedPage.remove(indexoftuple);	
		target.savePage(loadedPage);
		target.setNoOfRecords(target.getNoOfRecords()-1 );
				
		if(target.getNoOfRecords () == 0 && pages.size() > 1) 
		    this.updatePagesDelete(target);		   
				
	}
	
	public void updatePagesDelete(Page page){
		Page next;
		int pageNumber= page.getPageNumber();
		
		for(int i = pageNumber ; i < this.pages.size()-1 ;i++) {
		    next=this.pages.get(this.name+(i+1));
		    next.setPageNumber(i);
			page=this.pages.replace(this.name+i,next); 			
		}
		pages.remove(this.name+(this.pages.size()-1));
		
		
	}
   
    public void updateTuple(Object strClusteringKeyValue, Hashtable<String,Object> htblColNameValue) throws DBAppException {

		if (htblColNameValue.get(primaryKey)!=null)
			return;

		 Page p = this.getPageUpdate(strClusteringKeyValue);
		 int index = p.getIndexUpdate(this.primaryKey,strClusteringKeyValue);
		 Set<String> keys = this.colNameType.keySet();
		 p.updateTuple(index, htblColNameValue,keys);
	
	 }

    public Cell getCell(Hashtable<String,Object> htblColNameValue, Cell [] cells) throws DBAppException{
        Object value= htblColNameValue.get(cells[0].name);

	 	if(value==null)
	 		return cells[10];

		int left = 0, right = 9;
		int count=0;
		while(left<=right) {

			int middle = (left + right )/ 2;
			System.out.println("middle"+middle);

			if(compareTo(cells[middle].min,value) <= 0 && compareTo(cells[middle].max, value) > 0) {
				//System.out.println("first if");
                //System.out.println("value: "+value+"min: "+cells[middle].min+ "max: "+cells[middle].max);
				return cells[middle] ;
			}
			else if (compareTo(cells[middle].max,value) <0) {
				System.out.println("else if");
				left = middle + 1;
			}
			else {
				System.out.println("else");
				right = middle - 1;
			}
			count++;
			if (count==10)
				return null;
		}

		throw new DBAppException("something went wrong");

	}

	public void insertIntoGrid(Grid grid, Hashtable<String,Object> htblColNameValue , String filename) throws DBAppException{
		System.out.println("wtf");
        Cell cell=getCell(htblColNameValue,grid.gridIndex);

        while(cell.cells!=null){
			System.out.println("am I here??");
        	cell=getCell(htblColNameValue, cell.cells);
		}
        Object key= htblColNameValue.get(this.primaryKey);

        if(cell.bucket==null)
            cell.bucket=new Bucket(cell.cellNumber,0);
        cell.bucket.insertIntoBucket(key,filename);

	}

	public void insertIntoGrids(Hashtable<String,Object> htblColNameValue , String filename)throws DBAppException{

	 	for(int i=0; i<this.indexes.size();i++){
            System.out.println("I was here at grid "+i);
	 		this.insertIntoGrid(this.indexes.get(i),htblColNameValue,filename);
		}
	}

	public Vector<Hashtable<String,Object>> selectWithoutIndex(Vector<Object> input, String operand)throws DBAppException{

	 	Vector<Hashtable<String,Object>> result= new Vector<Hashtable<String,Object>>();
		Page current;
		Vector<Hashtable<String,Object>> temp=new Vector<Hashtable<String,Object>>();
		Set<String> keys= this.pages.keySet();

		for(String key: keys){
			current = pages.get(key);
			switch (operand){
				case "AND":
					temp=current.LinearSearchAND(input);
					break;
				case "OR":
					temp=current.LinearSearchOR(input);
					break;
				case "XOR":
					temp=current.LinearSearchXOR(input);
					break;

			}
			result.addAll(temp);
		}
		return result;
	}


	public Iterator AndOperands(Vector<Object> input){
	 	return null;
	}

	public Hashtable <String,Object> getGridExtra(Vector<Hashtable<String,Object>> gridTerms){

	 	Vector term= (Vector) gridTerms.get(0).get("sqlTerm");
	 	int max= term.size();
	 	for(int i=1;i<gridTerms.size(); i++){
			term= (Vector) gridTerms.get(i).get("sqlTerm");
	 		if(term.size()>max){

			}
		}

	 	return null;
	}

	public Iterator OrOperands(Vector<Object> input){

		Vector<Hashtable<String,Object>> gridTerms=new Vector<Hashtable<String,Object>>();
		Boolean [] isUsed= new Boolean[input.size()];
		HashTuples result= this.filterResults(input);

		for(int i=0; i<this.indexes.size();i++){

			Vector <SQLTerm> intermediate= new Vector<SQLTerm>();
			Vector <SQLTerm> exclude= new Vector<SQLTerm>();

			for(int j=0;j<this.indexes.get(i).ranges.length;j++){ //make sure of length

				for(int k=0; j< input.size(); k++){

					SQLTerm term= (SQLTerm) input.get(k);
					String columnname= term.getColumnName();

					if(this.indexes.get(i).ranges[j][0].equals(columnname)) {

						if(isUsed[k]==true)
							exclude.add(term);
						intermediate.add(term);
						isUsed[k]=true;
					}

				}
			}
			if(intermediate.size()==0)
				continue;
			Hashtable<String,Object> gridMatch=new Hashtable<String,Object>();
			gridMatch.put("index",this.indexes.get(i));
			gridMatch.put("sqlTerms",(Object)intermediate);
			gridMatch.put("exclude",(Object)exclude);
			gridTerms.add(gridMatch);

		}


		return null;
	}

	public HashTuples filterResults(Vector<Object> input) {

		// 1.remove partial result (Hashtuple objects) from input
		HashTuples result = new HashTuples();
		int k = 0;
		while(true) {
			if (k < input.size()) {
				if(input.get(k) instanceof HashTuples){
					HashTuples PR= (HashTuples) input.get(k);
					while (PR.hasNext()){
						result.put(PR.next().get(this.primaryKey),PR.next());
					}
					k++;
				}
				else {
					input.remove(input.get(k));
					k--;
				}
			}
			else{
				break;
			}
		}
		//2. sum all partial result intp a hashtuple and return it
		return result;
	}

	public HashTuples or (Grid grid, Vector <SQLTerm> terms, Vector<SQLTerm> exclude){

        SQLTerm reverse;
        HashTuples result= new HashTuples();

	     for(int i=0;i<terms.size();i++){

	         Vector<SQLTerm> andTerms= new Vector<SQLTerm>();
	         andTerms.add(terms.get(i));

	         for (int j=i;j>0;j--) {

                reverse= reverseSqlTerm(terms.get(j));
                andTerms.add(reverse);
             }
	         for (int k=0;k<exclude.size();k++){

	             reverse=reverseSqlTerm(exclude.get(i));
                 andTerms.add(reverse);

             }
	         //call AND
         }
	 	return null;
	}

	public Iterator XorOperands(Vector<Object> input){
		return null;
	}

    public SQLTerm reverseSqlTerm(SQLTerm term){
	     return null;
    }





	/*public static void main(String[]args) throws DBAppException{
		
		String strTableName = "Student";
		Hashtable <String,String>htblColNameType = new Hashtable<String,String>( );
		Hashtable htblColNameMin = new Hashtable( );
		Hashtable htblColNameMax = new Hashtable( );
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.double");

		Hashtable input1 = new Hashtable( );
		input1.put("id", 1);//add value you want to add here
		input1.put("name", "yassmina");
		input1.put("gpa", 2.0);
		
		Hashtable input2 = new Hashtable( );
		input2.put("id", 3);//add value you want to add here
		input2.put("name", "alaa");
		input2.put("gpa", 2.0);
		
		Hashtable input3 = new Hashtable( );
		input3.put("id", 4);//add value you want to add here
		input3.put("name", "yassmina");
		input3.put("gpa", 2.0);
		
		Hashtable input4 = new Hashtable( );
		input4.put("id", 6);//add value you want to add here
		input4.put("name", "alaa");
		input4.put("gpa", 2.0);
		
		Hashtable input5 = new Hashtable( );
		input5.put("id", 8);//add value you want to add here
		input5.put("name", "alaa");
		input5.put("gpa", 2.0);
		
		Hashtable input6 = new Hashtable( );
		input6.put("id", 10);//add value you want to add here
		input6.put("name", "yasmsina");
		input6.put("gpa", 1.0);
		
		Hashtable input7 = new Hashtable( );
		input7.put("id", 12);//add value you want to add here
		input7.put("name", "alaa");
		input7.put("gpa", 1.0);
		
		Hashtable input8 = new Hashtable( );
		input8.put("id", 13);//add value you want to add here
		input8.put("name", "alaa");
		input8.put("gpa", 1.0);
		
		Hashtable input9 = new Hashtable( );
		input9.put("id", 15);//add value you want to add here
		input9.put("name", "alaa");
		input9.put("gpa", 1.0);
		
		Hashtable input10 = new Hashtable( );
		input10.put("id", 18);//add value you want to add here
		input10.put("name", "alaa");
		input10.put("gpa", 1.0);
		
		Hashtable input11 = new Hashtable( );
		input11.put("id", 25);//add value you want to add here
		input11.put("name", "alaa");
		input11.put("gpa", 1.0);
		
		Hashtable input12 = new Hashtable( );
		input12.put("id", 30);//add value you want to add here
		input12.put("name", "alaa");
		input12.put("gpa", 1.0);
		
		Hashtable input13 = new Hashtable( );
		input13.put("id", 33);//add value you want to add here
		input13.put("name", "alaa");
		input13.put("gpa", 1.0);
		
		Hashtable input14 = new Hashtable( );
		input14.put("id", 35);//add value you want to add here
		input14.put("name", "alaa");
		input14.put("gpa", 1.0);
		Hashtable input15 = new Hashtable( );
		input15.put("id", 38);//add value you want to add here
		input15.put("name", "alaa");
		input15.put("gpa", 1.0);
		
		Hashtable input16 = new Hashtable( );
		input16.put("id", 40);//add value you want to add here
		input16.put("name", "alaa");
		input16.put("gpa", 1.0);
		
		
		//Hashtable delete = new Hashtable( );	
		//delete.put("name", "alaa" );
		//delete.put("gpa", 2.0 );
		
		
			
		
		
		Table table= new Table(strTableName,"id",htblColNameType,5);
		table.insertTuple(input1);
		table.insertTuple(input2);
		table.insertTuple(input3);
		table.insertTuple(input4);
		table.insertTuple(input5);
		table.insertTuple(input6);
		table.insertTuple(input7);
		table.insertTuple(input8);
		table.insertTuple(input9);
		table.insertTuple(input10);
		table.insertTuple(input11);
		table.insertTuple(input12);
		table.insertTuple(input13);
		table.insertTuple(input14);
		table.insertTuple(input15);
		//table.insertTuple(input16);
		
		Hashtable update1 = new Hashtable( );	
		update1.put("name", "nadeen" );
		
		Hashtable update2 = new Hashtable( );	
		update2.put("name", "nadeen" );
		update2.put("gpa", 0.5 );
		
		
		//table.deleteTuples(delete);
		//table.updateTuple(1, update1 );
		//table.updateTuple(13, update2);
		
		
		
		//delete page 1
		//table.deleteTuple(input6);
		//table.deleteTuple(input7);
		////table.deleteTuple(input8);
		//table.deleteTuple(input9);
		//table.deleteTuple(input10);
		
		//table.deleteTuple(input1);
		//table.deleteTuple(input2);
		//table.deleteTuple(input3);
		//table.deleteTuple(input4);
		//table.deleteTuple(input5);
		
		//delete last page
		//table.deleteTuple(input11);
		//table.deleteTuple(input7);		
		//table.deleteTuple(input9);
		
		table.deleteTuple(input10);
		table.deleteTuple(input12);
		table.deleteTuple(input8);
		table.deleteTuple(input3);		
		table.deleteTuple(input1);
		
		System.out.println("after insert:   ");

		Page p;
    	Set<String> keys= table.pages.keySet();
    	
	    ////////display  
		for(String key: keys){
			
			p=table.pages.get(key);
			
			System.out.println("Page:"+key+p.loadPage());
			
	     }	
		System.out.println("");



		Vector<Object>test = new Vector<Object>();
		SQLTerm[] arrSQLTerms;
		arrSQLTerms = new SQLTerm[2];
		arrSQLTerms[0]._strTableName = "Student";
		arrSQLTerms[0]._strColumnName= "name";
		arrSQLTerms[0]._strOperator = "=";
		arrSQLTerms[0]._objValue = "John Noor";
		arrSQLTerms[1]._strTableName = "Student";
		arrSQLTerms[1]._strColumnName= "gpa";
		arrSQLTerms[1]._strOperator = "=";
		arrSQLTerms[1]._objValue = new Double( 1.5 );
		String[]strarrOperators = new String[1];
		strarrOperators[0] = "OR";
		test.add(arrSQLTerms[0]);
		test.add(arrSQLTerms[1]);
		table.selectWithPK(arrSQLTerms[0],test);









	}*/
	
	

}
