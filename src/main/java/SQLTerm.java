public class SQLTerm {

    String _strTableName;
    String _strColumnName;
    String _strOperator; //<
    Object _objValue;

    public String getColumnName(){
        return this._strColumnName;
    }

    public SQLTerm(){ }

    public SQLTerm(String _strTableName,  String _strColumnName, String _strOperator, Object _objValue){

        this._strTableName=_strTableName;
        this._strColumnName=_strColumnName;
        this._strOperator=_strOperator;
        this._objValue=_objValue;

    }
    public  Boolean isEqual (SQLTerm term2){
        if(this._strColumnName.equals(term2._strColumnName) && this._strOperator.equals(term2._strColumnName) &&
                this._objValue.equals(term2._objValue) && this._strTableName.equals(term2._strTableName))
            return true;
        else
            return false;
    }

}
