import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;
import java.util.Date;


public class Page implements Serializable{
	String name;
	public Serializable max;
	public Serializable min;
	int noOfRecords;
	int MaximumCapacity;
	int pageNumber;
	
	public Page(String tableName, int number, int N){
		
		this.name=tableName+number;
		noOfRecords=0;
		pageNumber=number;
		this.MaximumCapacity=N;

		Vector<Hashtable<String,Object>> tuples= new Vector<Hashtable<String,Object>>();
		this.savePage(tuples);
			
	}

   
	
	public void setMin(Object value){
		this.min=(Serializable) value;
		
	}

	public void setMax(Object value){
		this.max= (Serializable) value;
		
	}

	public void setNoOfRecords(int number){
		this.noOfRecords=number;
		
	}

	public int getNoOfRecords (){
		return this.noOfRecords;
	}

	public String getName() {
		return name;
	}

	public int getPageNumber() {
		return pageNumber;
	}

	public void setName(String newName) {
		this.name = newName;
	}

	public void setPageNumber(int number) {
		this.pageNumber=number;
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
					return ((Date)a).compareTo((Date)b);

				default:
					throw new DBAppException("Invalid Data Type");

			}
		}
      }
	
	
    public Vector<Hashtable<String,Object>> loadPage (){
		
		try{
			FileInputStream fs=new FileInputStream("./src/main/resources/data/"+this.name);
			ObjectInputStream is=new ObjectInputStream(fs);//chaining in action
			@SuppressWarnings("unchecked")
			Vector <Hashtable<String, Object>> page= (Vector<Hashtable<String, Object>>) is.readObject();
			is.close();
			return page;
			
		}
		catch (Exception ex){
			ex.printStackTrace();
		}
				
		return null;
		
	}
	
    public void savePage(Vector<Hashtable<String, Object>> page){
		
		try{
			
			FileOutputStream fs=new FileOutputStream("./src/main/resources/data/"+this.name);
			ObjectOutputStream os=new ObjectOutputStream(fs);
			os.writeObject(page);
			os.close();		
		}
		
		catch (Exception ex){
			
			ex.printStackTrace();
		}	
	}
    
    public int getIndexDelete (String key, Hashtable h) throws DBAppException{
	   
	   Vector<Hashtable<String,Object>> tuples=this.loadPage();
	   System.out.print(tuples);
		Object pk =  h.get(key);
		int high =tuples.size()-1;
		int low = 0;
		int mid;
		Object tempPK;
		
		while(high >= low) {
				mid =low + (high-low)/2;
				Hashtable Ve = (Hashtable) tuples.get(mid);
				tempPK =  Ve.get(key);
				if(compareTo(tempPK,pk)==0) {
					return mid ;
				}
				else if (compareTo(tempPK,pk)<0) {
					low = mid + 1;
				}
				else {
					high = mid -1;
				}
			}
		
		throw new DBAppException("Input tuple doesn't exist");		
	}
   
    public int getIndexUpdate(String primaryKey, Object keyVal) throws DBAppException{
	   	
   	Vector<Hashtable<String,Object>> tuples=this.loadPage();
	   	//System.out.print(tuples);
  
		int high =tuples.size()-1;
		int low = 0;
		int mid;
		Object tempPK;
		
		while(high >= low) {
				mid =low + (high-low)/2;
				tempPK = tuples.get(mid).get(primaryKey);
				
				if(compareTo(tempPK,keyVal)==0) {//compare
					return mid ;
				}
				else if (compareTo(tempPK,keyVal)<0) {//compare
					low = mid + 1;
				}
				else {
					high = mid -1;
				}
			}
		throw new DBAppException("index not found");
                                  
    }
   
    public int getIndexInsert (String key, Hashtable h) throws DBAppException{
	   
	   Vector<Hashtable<String,Object>> tuples=this.loadPage();
	    System.out.print(tuples);
		Object pk =  h.get(key);
	   System.out.println("key is:"+key);
		int high =tuples.size()-1;
		int low = 0;
		int mid;
		Object tempPK;
		
		while(high >= low) {
				mid =low + (high-low)/2;
				Hashtable Ve = (Hashtable) tuples.get(mid);
				tempPK =  Ve.get(key);
				if(compareTo(tempPK,pk)==0) {
					throw new DBAppException("invalid id already exists");	
					
				}
				else if (compareTo(tempPK,pk)<0) {
					low = mid + 1;
				}
				else {
					high = mid -1;
				}
			}
		return low;
	}
   
    public Vector<Hashtable<String,Object>> linearSearchPage(Hashtable<String,Object> input){
	   
	   Vector<Hashtable<String,Object>> output=new Vector<Hashtable<String,Object>>();
	   Vector<Hashtable<String,Object>> tuples=this.loadPage();
	   
	   for(int i=0; i<tuples.size();i++){
		   Boolean valid=true;
		   
		   Set<String> keys= input.keySet();
		   
		   Hashtable<String,Object> current= tuples.get(i);
		   
			for(String key: keys){
				
				if(!input.get(key).equals(current.get(key))){
					valid=false;
					break;		
				}
				
		     }
			if(valid){
				output.add(current);				
			}						   
	   }
	   this.savePage(tuples);
	   return output;
     }

	public Vector<Hashtable<String,Object>> LinearSearchAND(Vector<Object> input) throws DBAppException {

		System.out.println("I was in the linearSearch AND");
		Vector<Hashtable<String,Object>> result=new Vector<Hashtable<String,Object>>();
		Vector<Hashtable<String,Object>> tuples=this.loadPage();
		Hashtable<String,Object> current;

		for(int i=0; i<tuples.size();i++){
			Boolean flag=true;
			current= tuples.get(i);
			System.out.println("current is"+current);

			for(int j=0;j<input.size();j++){


				SQLTerm term= (SQLTerm) input.get(j);
				String operator= term._strOperator;
				Object value= current.get(term._strColumnName);

				switch(operator){

					case ">":
						if(compareTo(value, term._objValue)<=0)
							flag=false;
						break;
					case ">=":
						if(compareTo(value, term._objValue)<0)
							flag=false;
						break;
					case "<":
						if(compareTo(value, term._objValue)>=0)
							flag=false;
						break;
					case "<=":
						if(compareTo(value, term._objValue)>0)
							flag=false;
						break;
					case "=":
						if(compareTo(value, term._objValue)!=0)
							flag=false;
						break;
					case "!=":
						if(compareTo(value, term._objValue)==0)
							flag=false;
						break;
				}
				if(flag==false)break;


			}
			if(flag==true) result.add(current);
			System.out.println("flag is "+flag);
		}
		return result;
	}

	public Vector<Hashtable<String,Object>> LinearSearchOR(Vector<Object> input)throws DBAppException{

		Vector<Hashtable<String,Object>> result=new Vector<Hashtable<String,Object>>();
		Vector<Hashtable<String,Object>> tuples=this.loadPage();
		Hashtable<String,Object> current;

		for(int i=0; i<tuples.size();i++){
			current= tuples.get(i);
			Boolean flag=false;
			for(int j=0;j<input.size();j++){

				SQLTerm term= (SQLTerm) input.get(j);
				String operator= term._strOperator;
				Object value= current.get(term._strColumnName);

				switch(operator){

					case ">":
						if(compareTo(value, term._objValue)>0)
							flag=true;
						break;
					case ">=":
						if(compareTo(value, term._objValue)>=0)
							flag=true;
						break;
					case "<":
						if(compareTo(value, term._objValue)<0)
							flag=true;
						break;
					case "<=":
						if(compareTo(value, term._objValue)<=0)
							flag=true;
						break;
					case "=":
						if(compareTo(value, term._objValue)==0)
							flag=true;
						break;
					case "!=":
						if(compareTo(value, term._objValue)!=0)
							flag=true;
						break;
				}
				if(flag==true)break;

			}
			if(flag==true) result.add(current);

		}
		return result;
	}

	public Vector<Hashtable<String,Object>> LinearSearchXOR(Vector<Object> input)throws DBAppException{

		Vector<Hashtable<String,Object>> result=new Vector<Hashtable<String,Object>>();
		Vector<Hashtable<String,Object>> tuples=this.loadPage();
		Hashtable<String,Object> current;

		for(int i=0; i<tuples.size();i++){
			current= tuples.get(i);
			int count=0;
			for(int j=0;j<input.size();j++){

				SQLTerm term= (SQLTerm) input.get(j);
				String operator= term._strOperator;
				Object value= current.get(term._strColumnName);

				switch(operator){

					case ">":
						if(compareTo(value, term._objValue)>0)
							count++;
						break;
					case ">=":
						if(compareTo(value, term._objValue)>=0)
							count++;
						break;
					case "<":
						if(compareTo(value, term._objValue)<0)
							count++;
						break;
					case "<=":
						if(compareTo(value, term._objValue)<=0)
							count++;
						break;
					case "=":
						if(compareTo(value, term._objValue)==0)
							count++;
						break;
					case "!=":
						if(compareTo(value, term._objValue)!=0)
							count++;
						break;
				}


			}
			if(count%2!=0) result.add(current);
		}
		return result;
	}



    public Hashtable<String,Object> insertIntoPage(Hashtable<String,Object> input,int index, String key){
    	
    	Vector<Hashtable<String,Object>> tuples=this.loadPage();
    	Hashtable<String,Object> temp=null;
    	
    	if(index==0){ 		
    		this.min= (Serializable) input.get(key);
    	}
    	
    	
    	if(this.noOfRecords==0){
    		
            this.min= (Serializable)input.get(key);
            this.max= (Serializable)input.get(key);
    		tuples.add(input);
    	}
    	
    	
    	else if(this.noOfRecords==this.MaximumCapacity){
    		
    		
    		if(index>tuples.size()-1) {	  
    	     	return input;
    		}
    		
    	     	   	    
    		 temp= tuples.get(noOfRecords-1);
    	
    	   for(int i=noOfRecords-2; i>=index; i--) 	   		    
    	      tuples.set(i+1,tuples.get(i));		
    	       	   
    	    tuples.set(index,input);
    	    this.savePage(tuples);
    	    this.max= (Serializable) tuples.get(tuples.size()-1).get(key);
    	    return temp;
    	}
    	
    	else{  
    		
    	  if(index>tuples.size()-1) {	
    		 this.max= (Serializable) input.get(key);
    		 tuples.add(index,input);
    	  }
    		
          for(int i=noOfRecords-1; i>=index; i--){ 
    	  
        	  if(i==noOfRecords-1){
        		 tuples.add(i+1,tuples.get(i));
        		 continue;
        	  }       		  	  		
    	      tuples.set(i+1,tuples.get(i)); 
    	  }
          
          tuples.set(index,input);
         
          
        }
    	 noOfRecords++;

         this.savePage(tuples);
    	 return temp;
    		    	    	
    	    	
    }
  
	
	public void updateTuple(int index, Hashtable<String,Object> updateTable,Set<String> keys){
    	Vector<Hashtable<String,Object>> tuples=this.loadPage();
        for(String key: keys){
            if (updateTable.containsKey(key)){
            	if (tuples.get(index).containsKey(key))
            		tuples.get(index).replace(key, updateTable.get(key));
            	else
            		tuples.get(index).put(key, updateTable.get(key));
            }
            this.savePage(tuples);
        }

    }
    
    
    /*public static void main(String [] args){
    	
    	Page page= new Page("Employee",0);

    	
    	Hashtable h1 = new Hashtable( );
		h1.put("id", new Integer( 1));
		h1.put("name", new String("mariam" ) );
		h1.put("gpa", new Double( 2.0 ) );
		
		Hashtable h2 = new Hashtable( );
		h2.put("id", new Integer( 5));
		h2.put("name", new String("Sara" ) );
		h2.put("gpa", new Double( 2.0 ) );
		
		Hashtable h3 = new Hashtable( );
		h3.put("id", new Integer( 8));
		h3.put("name", new String("Farida" ) );
		h3.put("gpa", new Double( 2.0 ) );
		
		Hashtable h4 = new Hashtable( );
		h4.put("id", new Integer( 16));
		h4.put("name", new String("nada" ) );
		h4.put("gpa", new Double( 1.8 ) );
		
		Hashtable h5 = new Hashtable( );
		h5.put("id", new Integer( 20));
		h5.put("name", new String("mariam" ) );
		h5.put("gpa", new Double( 2.0) );
		
		Vector <Hashtable<String,Object>> tuples =new Vector <Hashtable<String,Object>>();
		
		tuples.add(h1);
		tuples.add(h2);
		tuples.add(h3);
		tuples.add(h4);
		tuples.add(h5);
		
		
		System.out.println(tuples);
		page.savePage(tuples);
		page.noOfRecords=5;
				
		Hashtable h6 = new Hashtable( );
		h6.put("id", new Integer( 20));
		h6.put("gpa", new Double( 2.0 ) );
		h6.put("name", new String("mariam" ) );
		//h6.put("name", new String("mariam" ) );
		System.out.println("After ");
		
		System.out.println(page.linearSearchPage(h6));
				
		
    }*/


	
}
