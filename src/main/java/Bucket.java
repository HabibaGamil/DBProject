import java.awt.*;
import java.io.*;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

public class Bucket implements Serializable{

    Vector<Pointer> paths;
    String filename;
    int cellNumber;
    int bucketNumber;
    int maximumCapacity=3;
    int entries=0;
    Bucket overflow;
    Object minimum;
    Object maximum;



    public class Pointer implements Serializable {

        Object key;
        String path;

        public Pointer(Object key, String path){
            this.key=key;
            this.path=path;
        }
        public String getPath() {
            return path;
        }
        public Object getKey() {
            return this.key;
        }
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

    public Bucket(int cellNumber, int bucketNumber){

        this.paths=new Vector<Pointer>();
        String name ="Cell"+cellNumber+"Bucket"+bucketNumber;
        this.filename=name;
        this.cellNumber=cellNumber;
        this.bucketNumber=bucketNumber;
        this.saveBucket(paths);
    }

    public Vector<Pointer> loadBucket (){

        try{
            FileInputStream fs=new FileInputStream("./src/main/resources/data/"+this.filename);
            ObjectInputStream is=new ObjectInputStream(fs);//chaining in action
            @SuppressWarnings("unchecked")
            Vector <Pointer> bucket= (Vector<Pointer>) is.readObject();
            is.close();
            return bucket;

        }
        catch (Exception ex){
            ex.printStackTrace();
        }

        return null;

    }

    public void saveBucket(Vector <Pointer> bucket){

        try{

            FileOutputStream fs=new FileOutputStream("./src/main/resources/data/"+this.filename);
            ObjectOutputStream os=new ObjectOutputStream(fs);
            os.writeObject(bucket);
            os.close();
        }

        catch (Exception ex){
            ex.printStackTrace();
        }
    }

    public Bucket getBucketInsert(Object key) throws DBAppException{

        Bucket current=this;

        if(this.maximum==null || this.minimum==null){
            this.maximum=key;
            this.minimum=key;
        }

        while (current!=null){

            if(compareTo(this.minimum,key) > 0)
                return current;

            if((compareTo(current.minimum,key) <= 0 && compareTo(current.maximum, key) >=0))
                return current;

            if(current.overflow==null)
                return current;

            current=current.overflow;
        }
        return null;
        // {2,3,6,7,9} Edge cases// insert 1- 10- 20
        // {12,13,18}
    }

    public int getIndex(Object key) throws DBAppException{

        Vector <Pointer> tuples=this.loadBucket();

        System.out.println("key is:"+key);
        System.out.print(tuples);

        int high =tuples.size()-1;
        int low = 0;
        int mid;
        Object tempPK;

        while(high >= low) {
            mid =low + (high-low)/2;
            Pointer current = (Pointer) tuples.get(mid);
            tempPK =  current.key;

            if(compareTo(tempPK,key)==0) {
                throw new DBAppException("invalid id already exists");

            }
            else if (compareTo(tempPK,key)<0) {
                low = mid + 1;
            }
            else {
                high = mid -1;
            }
        }
        return low;
    }

    public Pointer insert (Pointer input , int index){

        Vector<Pointer> pointers =this.loadBucket();
        Pointer temp=null;

        if(index==0){
            this.minimum= input.key;
        }

        if(this.entries==0){

            this.minimum= input.key;
            this.maximum= input.key;
            pointers.add(input);
        }


        else if(this.entries==this.maximumCapacity){

            if(index>pointers.size()-1) {
                return input;
            }
            temp= pointers.get(entries-1);

            for(int i=entries-2; i>=index; i--)
                pointers.set(i+1,pointers.get(i));

            pointers.set(index,input);
            this.saveBucket(pointers);
            this.maximum= pointers.get(pointers.size()-1).key;
            return temp;
        }

        else{

            if(index>pointers.size()-1) {
                this.maximum=  input.key;
                pointers.add(index,input);
            }

            for(int i=entries-1; i>=index; i--){

                if(i==entries-1){
                    pointers.add(i+1,pointers.get(i));
                    continue;
                }
                pointers.set(i+1,pointers.get(i));
            }

            pointers.set(index,input);


        }
        entries++;

        this.saveBucket(pointers);
        return temp;


    }

    public void insertIntoBucket(Object key, String path) throws DBAppException{

        Pointer pointer= new Pointer(key,path);
        Bucket bucket=this.getBucketInsert(key);
        System.out.println("bucket"+bucket);
        int index=bucket.getIndex(key);
        Pointer overflow= bucket.insert(pointer,index);

        while(overflow!=null){

            if(bucket.overflow==null){
                bucket.overflow=new Bucket(this.cellNumber ,(++this.bucketNumber));
            }
            bucket=bucket.overflow;
            overflow= bucket.insert(overflow,0);

        }

    }

    public void displayBucket(){
        Bucket current=this;
        while(current!=null){
            Vector<Pointer> pointers=current.loadBucket();
            System.out.println("Bucket: "+current.bucketNumber);
            for(int i=0; i<pointers.size();i++)
                System.out.println("KEY: "+ pointers.get(i).key+ ", path: "+ pointers.get(i).path);
            current=current.overflow;
        }
        System.out.println("end of bucket");
    }




}
