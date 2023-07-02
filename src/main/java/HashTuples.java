import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

public class HashTuples implements Iterator {

    int current=0;
    Vector<Object> keys;
    Hashtable<Object,Hashtable<String,Object>> tuples;

    public HashTuples(){

        this.keys=new Vector<Object>();
        this.tuples=new Hashtable<Object,Hashtable<String,Object>>();
    }

    public void put(Object key,Hashtable<String,Object> tuple){
        //check if key already exists in hashtable to avoid dublicate values
        if(tuples.get(key)==null) {
            tuples.put(key, tuple);
            keys.add(key);
        }
    }

    @Override
    public boolean hasNext() {
        if (keys.size()==0)
            return false;
        if(current>=keys.size()-1)
            return false;
        return true;
    }

    @Override
    public Hashtable<String,Object> next() {

        Hashtable<String,Object> next;
        if(this.hasNext()) {
            next = tuples.get(keys.get(current));
            this.current++;
            return next;
        }
        return null;

    }
}
