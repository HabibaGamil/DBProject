import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Hashtable;
import java.util.Vector;
import java.time.LocalDate;
import java.util.*;

public class Grid implements Serializable {

    String [][] ranges;
    Cell [] gridIndex;
    int noOfB;
    
    public Grid (String[][]ranges, Table table) throws ParseException, DBAppException{

        this.ranges=ranges;
        this.gridIndex= createGrid(ranges,0);
        //this.populateGrid(table);
        //this.displayGrid(this.gridIndex);
        this.noOfB=0;

    }

    public Cell[] createGrid (String [][] attRange, int index) throws ParseException {

        int cellNumber=0;
        Cell [] grid= new Cell[11];
        Vector<Hashtable<String,Object>> range1D=this.divideRanges(attRange[index][1],attRange[index][2],attRange[index][3]);

        if (attRange.length-1==index) {

            for(int i =0;i<10;i++){

                grid[i]= new Cell (attRange[index][0],range1D.get(i).get("min"),range1D.get(i).get("max"), null, cellNumber);
                cellNumber++;
            }
            grid[10]= new Cell ("undefined",null ,null, null, cellNumber);
            return grid;
        }
        else{

           for(int i =0;i<10;i++){

                grid[i]= new Cell (attRange[index][0],range1D.get(i).get("min"),range1D.get(i).get("max"), createGrid(attRange,index+1),-1);
           }
            grid[10]= new Cell ("undefined",null ,null, createGrid(attRange,index+1), cellNumber);
            return grid;
        }



    }

    public  Vector<Hashtable<String,Object>> divideRanges(String min, String max, String type) throws ParseException {

        String inputType=type.toLowerCase();
        Vector<Hashtable<String,Object>> columnRange=new Vector<Hashtable<String,Object>>();
        switch(inputType){

            case "java.lang.integer" :

                int minimumInt=Integer.parseInt(min);
                int maximumInt=Integer.parseInt(max);
                int difference= maximumInt-minimumInt;
                double currentInt= minimumInt;
                int step=difference/10;
                for(int i=0; i<9;i++){
                    Hashtable <String,Object> hash=new Hashtable<String,Object>();
                    hash.put("min",currentInt);
                    currentInt=currentInt+step;
                    hash.put("max",currentInt);
                    columnRange.add(hash);
                }
                Hashtable <String,Object> hashInt=new Hashtable<String,Object>();
                hashInt.put("min",currentInt);
                hashInt.put("max",maximumInt);
                columnRange.add(hashInt);
                return columnRange;

            case "java.lang.double" :

                double minimumDouble=Double.parseDouble(min);
                double maximumDouble=Double.parseDouble(max);
                double differenceD= maximumDouble-minimumDouble;
                double current= minimumDouble;
                double stepD=differenceD/10;
                System.out.println("step:"+stepD);
                for(int i=0; i<10;i++){
                    Hashtable <String,Object> hash=new Hashtable<String,Object>();
                    hash.put("min",current);
                    current=current+stepD;
                    hash.put("max",current);
                    columnRange.add(hash);
                }
                return columnRange;


            case "java.lang.string" :

                int uniCodeSumMin=0;
                int uniCodeSumMax=0;
                for(int i=0;i<min.length()-1;i++)
                    uniCodeSumMin+=min.codePointAt(i);

                for(int i=0;i<max.length()-1;i++)
                    uniCodeSumMax+=max.codePointAt(i);

                int differenceS=uniCodeSumMax-uniCodeSumMin;
                int stepS = differenceS/10;
                int currentStr=uniCodeSumMin;
                for(int i=0; i<9;i++){

                    Hashtable <String,Object> hash=new Hashtable<String,Object>();
                    hash.put("min",currentStr);
                    currentStr=currentStr+stepS;
                    hash.put("max",currentStr);
                    columnRange.add(hash);
                }
                Hashtable <String,Object> hashLast=new Hashtable<String,Object>();
                hashLast.put("min",currentStr);
                hashLast.put("max",uniCodeSumMax);
                columnRange.add(hashLast);

                return columnRange;

            case "java.util.date":

                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                Date dateMin =(Date) format.parse(min);
                Date dateMax = (Date) format.parse(max);
                long differenceDate = dateMax.getTime() - dateMin.getTime();
                long stepDate=differenceDate/10;
                long currentDate= dateMin.getTime();

                for(int i=0; i<10;i++){
                    Hashtable <String,Object> hash=new Hashtable<String,Object>();
                    Date minimum = new Date();
                    Date maximum = new Date();
                    minimum.setTime(currentDate);
                    currentDate=currentDate+stepDate;
                    maximum.setTime(currentDate);
                    hash.put("min",minimum);
                    hash.put("max",maximum);
                    columnRange.add(hash);
                }
                return columnRange;

            default:
                break;

        }

        return null;

    }

    public void populateGrid(Table table) throws DBAppException{

        Hashtable<String, Page> pages = table.pages;

        if (table.noOfPages == 0) { //zero bcz in ms2 initially there is no page 0
            return;
        } else {
            Set<String> keys = pages.keySet();
            Page p;
            for (String key : keys) {
                p = pages.get(key);
                Vector<Hashtable<String, Object>> tuples = p.loadPage();
                for (int i = 0; i < tuples.size(); i++) {
                    //table.insertIntoGrid(this, tuples.get(i),);
                }
            }
        }
    }

    public void displayGrid(Cell [] gridIndex){
       int number=0;
        for (int i=0; i<gridIndex.length;i++){

            if (gridIndex[i].cells==null){
                number++;
                System.out.println("Column: "+gridIndex[i].name+ "  Max: "+gridIndex[i].max+"Min: "+gridIndex[i].min);
                if( gridIndex[i].bucket!=null)
                gridIndex[i].bucket.displayBucket();
                else
                    System.out.println("no bucket");
            }
            else
            this.displayGrid(gridIndex[i].cells);

        }
        System.out.println("number of blocks"+number);
    }

    public static void main(String []args){

    }




}
