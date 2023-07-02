import java.nio.file.Path;
import java.util.Hashtable;
import java.util.Vector;

public class Dimension {

    String columnName;
    Object min;
    Object max;
    Bucket bucket;
    Dimension [] dimension;



    public Dimension( String columnName ,Object min ,Object max ,Dimension [] dimension){

        this.columnName=columnName;
        this.min=min;
        this.max=max;
        this.dimension=dimension;
        this.bucket=null;
    }

    public String getColumnName(){
        return this.columnName;
    }

}
