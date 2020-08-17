package EMFSQL;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
public class FileGenerator {
	
	private static final String EMF_STRUCTURE_CLASSNAME = NameDefine.EMF_STRUCTURE_CLASSNAME;
	private static final String EMFQUERY_PROCESSOR_CLASSNAME = NameDefine.EMFQUERY_PROCESSOR_CLASSNAME;
	private static final String EMFQUERY_PROCESSOR_FILEPATH = NameDefine.EMFQUERY_PROCESSOR_FILEPATH;
	private static final String EMFTABLE_FIELD = NameDefine.EMFTABLE_FIELD;
	private static final String PACKAGE_NAME = NameDefine.PACKAGE_NAME;
	private static final String EMF_STRUCTURE_FILEPATH = NameDefine.EMF_STRUCTURE_FILEPATH;
	
	public static boolean EMFStructureGenerator(Map<String, String> EmftableDataTypeMap) {

		File file = new File(EMF_STRUCTURE_FILEPATH); //object for output file
		PrintWriter pw = null;
		try {
			pw = new PrintWriter(file);
			
			//write program package and import information to the output file
			pw.println("package " + PACKAGE_NAME + ";");
			pw.println("public class " + EMF_STRUCTURE_CLASSNAME + " {");

			//write all the required variables from the Map.
			Set<Entry<String, String>> entries = EmftableDataTypeMap.entrySet();
			for (Entry<String, String> entry : entries)
				pw.println(" public " + entry.getValue() + " " + entry.getKey() + ";");
			pw.println("}");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return false;
		} finally {
			//close file resource
			if (pw != null)
				pw.close();
		}
		System.out.println("EMF_Structure Generate Success!\n");
		return true;
	}
	
	public static boolean GenerateQPClass(Map<String, String> input, Map<String, String> EmfDataTypeMap) {
		File file = new File(EMFQUERY_PROCESSOR_FILEPATH);
		PrintWriter pw = null;
		String[] gas = input.get(NameDefine.GROUPING_ATTRIBUTES).split(",");
		List<String> atts = new ArrayList<String>();	// Attributes that we need
		boolean aggonatt = false;	// If we have aggregation on attributes

		//loop for each item in the emf structure
		Set<Entry<String, String>> entries = EmfDataTypeMap.entrySet();
		for (Entry<String, String> entry : entries) {
			String k = entry.getKey();
			
			//condition to check if current value is an aggregate function
			if (k.startsWith("sum") || k.startsWith("count") || k.startsWith("min") || k.startsWith("max") || k.startsWith("avg")) {
				String str = "";
				int count = count_(k, "_"); //count the number of "_" in the variable name to between grouping attributes and grouping variables
				//count == 1 means the variable is a grouping attributes, else it is a grouping variable
				if (count == 1) { 
					aggonatt = true;
					str = k.substring(k.indexOf('_') + 1);	// get the attribute name
				} else if (count > 1) { 
					str = k.substring(k.indexOf('_') + 1, k.lastIndexOf('_'));
				}

				if (!atts.contains(str))
					atts.add(str);
			}
		}

		try {
			//write program package and import information to the output file
			pw = new PrintWriter(file);
			pw.println("package " + PACKAGE_NAME + ";");
			pw.println("import " + PACKAGE_NAME + "." + EMF_STRUCTURE_CLASSNAME + ";");
			pw.println("import " + PACKAGE_NAME + "." + "DBProcessing" + ";");
			pw.println("import " + "java.sql.ResultSet" + ";");
			pw.println("import " + "java.sql.SQLException" + ";");
			pw.println("public class " + EMFQUERY_PROCESSOR_CLASSNAME + " {");
			pw.println("private " + EMF_STRUCTURE_CLASSNAME + "[] " + EMFTABLE_FIELD + " = new " + EMF_STRUCTURE_CLASSNAME + "[500];");
			pw.println("private int counter;"); // count the number of tuple

			// algorithm method
			pw.println("public void algorithm(){");
			pw.println("DBProcessing dbpr = new DBProcessing();");
			pw.println("String query = \"select * from sales\";");
			pw.println("ResultSet rs = dbpr.QueryExecute(query);");

			pw.println("try {");
			pw.println("while( rs.next()) {");
			pw.println("int index = 0; ");
			pw.println("boolean isexist = false;");	// If the tuple already exist in table

			String gacondition = "";	// get grouping attributes' condition
			for (int idx = 0; idx < gas.length; idx++) {

				if (EmfDataTypeMap.containsKey(gas[idx])) {
					String dataType = EmfDataTypeMap.get(gas[idx]);

					if (dataType.equals("String")) {
						pw.println("String " + gas[idx] + " = rs.getString(\"" + gas[idx] + "\");");
						gacondition = gacondition + "emftable[index]." + gas[idx] + ".equals(" + gas[idx] + ")";
					} else if (dataType.equals("int")) {
						pw.println("int " + gas[idx] + " = rs.getInt(\"" + gas[idx] + "\");");
						gacondition = gacondition + "emftable[index]." + gas[idx] + " == " + gas[idx];
					}

					if (idx < gas.length - 1)
						gacondition = gacondition + " && ";
				}

			}

			if (aggonatt) {
				for (String att : atts) {
					if(!ExIngaCheck(gas,att))	// Check if need extra attributes
						pw.println("int " + att + " = rs.getInt(\"" + att + "\");");
				}
			}
			pw.println("while (index < counter) { ");
			pw.println("if (" + gacondition + "){");

			if (aggonatt) {	// Update the result of grouping function on grouping attributes
				for (String att : atts) {
					if (EmfDataTypeMap.containsKey("sum_" + att)) {
						pw.println("emftable[index].sum_" + att + " = emftable[index].sum_" + att + " + " + att + ";");
					}
					if (EmfDataTypeMap.containsKey("count_" + att)) {
						pw.println("emftable[index].count_" + att + " = emftable[index].count_" + att + " + " + 1 + ";");
					}
					if (EmfDataTypeMap.containsKey("max_" + att)) {
						pw.println("if (emftable[index].max_" + att + " < " + att + ")");
						pw.println("emftable[index].max_" + att + " = " + att + ";");
					}
					if (EmfDataTypeMap.containsKey("min_" + att)) {
						pw.println("if (emftable[index].min_" + att + " > " + att + ")");
						pw.println("emftable[index].min_" + att + " = " + att + ";");
					}
					if (EmfDataTypeMap.containsKey("avg_" + att)) {
						pw.println("emftable[index].avg_" + att + " = emftable[index].sum_" + att + "/emftable[index].count_" + att + ";");
					}
				}
			}
			pw.println("isexist = true ;");
			pw.println("break;");

			pw.println("}"); // end if gacondition
			pw.println("index++;");
			pw.println("}");// end while

			pw.println("if (!isexist) {");
			pw.println("emftable[counter] = new " + EMF_STRUCTURE_CLASSNAME + "();");
			for (int idx = 0; idx < gas.length; idx++) {
				pw.println("emftable[counter]." + gas[idx] + " = " + gas[idx] + ";");
			}

			if (aggonatt) {

				for (String att : atts) {
					if (EmfDataTypeMap.containsKey("count_" + att))
						pw.println("emftable[index].count_" + att + " = 1;");
					if ((EmfDataTypeMap.containsKey("sum_" + att)))
						pw.println("emftable[index].sum_" + att + " = " + att + ";");
					if ((EmfDataTypeMap.containsKey("max_" + att)))
						pw.println("emftable[index].max_" + att + " = " + att + ";");
					if ((EmfDataTypeMap.containsKey("min_" + att)))
						pw.println("emftable[index].min_" + att + " = " + att + ";");
					if ((EmfDataTypeMap.containsKey("avg_" + att)))
						pw.println("emftable[index].avg_" + att + " = emftable[index].sum_" + att + "/emftable[index].count_" + att + ";");
				}
			}
			pw.println("counter++;");

			pw.println("}"); // end if exist

			pw.println("}"); // while rs

			// Start looping for grouping variables
			String str_gv = input.get(NameDefine.NO_OF_GRP_VAR);
			String[] select_condition_vect = input.get(NameDefine.SELECT_CONDITION_VECT).split(",");

			if (str_gv.length() > 0) {
				int no_of_gv = Integer.parseInt(str_gv);

				for (int i = 0; i < no_of_gv; i++) {

					pw.println("rs.beforeFirst();");
					pw.println("while( rs.next()) {");
					pw.println("int index = 0; ");

					for (int idx = 0; idx < gas.length; idx++) {

						if (EmfDataTypeMap.containsKey(gas[idx])) {
							String dataType = EmfDataTypeMap.get(gas[idx]);

							if (dataType.equals("String"))
								pw.println("String " + gas[idx] + " = rs.getString(\"" + gas[idx] + "\");");
							else if (dataType.equals("int"))
								pw.println("int " + gas[idx] + " = rs.getInt(\"" + gas[idx] + "\");");
						}
					}

					for (String col : atts) {
						if(!ExIngaCheck(gas,col))
							pw.println("int " + col + " = rs.getInt(\"" + col + "\");");
					}

					pw.println("while (index < counter) { ");
					// Such That Conditions
					pw.println("if (" + select_condition_vect[i] + "){");

					for (String att : atts) {
						String att_name = att + "_" + (i + 1);	// order of gv is same in condition input
						if (EmfDataTypeMap.containsKey("sum_" + att_name)) {
							pw.println("emftable[index].sum_" + att_name + " = emftable[index].sum_" + att_name + " + " + att + ";");
						}
						if (EmfDataTypeMap.containsKey("count_" + att_name)) {
							pw.println("emftable[index].count_" + att_name + " = emftable[index].count_" + att_name + " + " + 1 + ";");
						}

						if (EmfDataTypeMap.containsKey("max_" + att_name)) {
							pw.println("if (emftable[index].max_" + att_name + " < " + att + ")");
							pw.println("emftable[index].max_" + att_name + " = " + att + ";");
						}

						if (EmfDataTypeMap.containsKey("min_" + att_name)) {
							pw.println("if (emftable[index].min_" + att_name + " > " + att + ")");
							pw.println("emftable[index].min_" + att_name + " = " + att + ";");
						}

						if (EmfDataTypeMap.containsKey("avg_" + att_name)) {
							pw.println("emftable[index].avg_" + att_name + " = emftable[index].sum_" + att_name + "/emftable[index].count_" + att_name + ";");
						}
					}
					
					pw.println("}"); // end if select_condition_vect
					pw.println("index++;");
					pw.println("}");// end while
					pw.println("}"); // end while rs
				}
			}

			pw.println("}"); // try end
			pw.println("catch (SQLException e) {");
			pw.println("e.printStackTrace();");
			pw.println("}"); // catch end
			pw.println("}"); // method end;

			String[] displayatts = input.get(NameDefine.SELECT_ATTRIBUTES).split(",");
			String disatt = "";
			String headeratt = "";
			
			// Display result function.
			pw.println("public void displayResult(){ ");

			for (int idx = 0; idx < displayatts.length; idx++) {

				headeratt = headeratt + displayatts[idx];
				if (displayatts[idx].startsWith("emftable"))		//make sure starts with "emftable"
					disatt = disatt + displayatts[idx];
				else
					disatt = disatt + "emftable[index]." + displayatts[idx];

				if (idx < displayatts.length - 1) {
					disatt = disatt + "+\"\t|\t\"+";
					headeratt = headeratt + "\t|\t";
				}

			}

			pw.println("System.out.println(\"" + headeratt + "\");");
			pw.println("System.out.println(\"----------------------------------------------------------------\");");
			pw.println("for (int index = 0; index < counter; index++) {");
			String havingCondition = input.get(NameDefine.HAVING_CONDITION);

			if (!havingCondition.isEmpty() && havingCondition.length() > 0) {
				havingCondition = havingCondition.replace("sum", "emftable[index].sum");
				havingCondition = havingCondition.replace("count", "emftable[index].count");
				havingCondition = havingCondition.replace("max", "emftable[index].max");
				havingCondition = havingCondition.replace("min", "emftable[index].min");
				havingCondition = havingCondition.replace("avg", "emftable[index].avg");
				pw.println("if(" + havingCondition + ") {");
			}

			pw.println("System.out.println(" + disatt + ");");

			if (havingCondition.length() > 0)
				pw.println("}");

			pw.println("}");
			pw.println("}");
			
			// Add main method in the QP class
			pw.println("public static void main(String[] args){");
			pw.println("System.out.println(\"QP class main()\");");
			pw.println(EMFQUERY_PROCESSOR_CLASSNAME + " qp = new " + EMFQUERY_PROCESSOR_CLASSNAME + "();");
			pw.println("qp.algorithm();");
			pw.println("qp.displayResult();");
			pw.println("}");
			pw.println("}");
			
			System.out.println("Query Processor Generate Success!\n");
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return false;
		} finally {
			if (pw != null)
				pw.close();
		}

		return true;
	}
	
	// Function to count "_"
	public static int count_(String str1,String str2){
		int count = str1.length() - str1.replace(str2, "").length();
		return count;
	}
	
	public static boolean ExIngaCheck(String[] ga, String att){
		
		for(String a : ga){
			if(a.equalsIgnoreCase(att))
				return true;
		}
		return false;
	}
}
