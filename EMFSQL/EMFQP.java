package EMFSQL;
import EMFSQL.EMFStructure;
import EMFSQL.DBProcessing;
import java.sql.ResultSet;
import java.sql.SQLException;
public class EMFQP {
private EMFStructure[] emftable = new EMFStructure[500];
private int counter;
public void algorithm(){
DBProcessing dbpr = new DBProcessing();
String query = "select * from sales";
ResultSet rs = dbpr.QueryExecute(query);
try {
while( rs.next()) {
int index = 0; 
boolean isexist = false;
String prod = rs.getString("prod");
int month = rs.getInt("month");
while (index < counter) { 
if (emftable[index].prod.equals(prod) && emftable[index].month == month){
isexist = true ;
break;
}
index++;
}
if (!isexist) {
emftable[counter] = new EMFStructure();
emftable[counter].prod = prod;
emftable[counter].month = month;
counter++;
}
}
rs.beforeFirst();
while( rs.next()) {
int index = 0; 
String prod = rs.getString("prod");
int month = rs.getInt("month");
int quant = rs.getInt("quant");
while (index < counter) { 
if (emftable[index].prod.equals(prod) && emftable[index].month == month){
emftable[index].sum_quant_1 = emftable[index].sum_quant_1 + quant;
}
index++;
}
}
rs.beforeFirst();
while( rs.next()) {
int index = 0; 
String prod = rs.getString("prod");
int month = rs.getInt("month");
int quant = rs.getInt("quant");
while (index < counter) { 
if ( emftable[index].prod.equals(prod)){
emftable[index].sum_quant_2 = emftable[index].sum_quant_2 + quant;
}
index++;
}
}
}
catch (SQLException e) {
e.printStackTrace();
}
}
public void displayResult(){ 
System.out.println("prod	|	month	|	emftable[index].sum_quant_1 * 1.0/emftable[index].sum_quant_2");
System.out.println("----------------------------------------------------------------");
for (int index = 0; index < counter; index++) {
System.out.println(emftable[index].prod+"	|	"+emftable[index].month+"	|	"+emftable[index].sum_quant_1 * 1.0/emftable[index].sum_quant_2);
}
}
public static void main(String[] args){
System.out.println("QP class main()");
EMFQP qp = new EMFQP();
qp.algorithm();
qp.displayResult();
}
}
