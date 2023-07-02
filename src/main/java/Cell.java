import java.io.Serializable;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

public class Cell implements Serializable{

    String name;
    int cellNumber;
    Object min;
    Object max;
    Bucket bucket;
    Cell [] cells;



    public Cell( String columnName ,Object min ,Object max ,Cell [] cells, int cellNumber){

        this.name=columnName;
        this.min=min;
        this.max=max;
        this.cells=cells;
        this.cellNumber=cellNumber;
    }

    public String getColumnName(){
        return this.name;
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




}
