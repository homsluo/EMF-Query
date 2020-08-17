package EMFSQL;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.TreeMap;
public class Main {
	private Map<String, String> input;
	private static final String INPUT_FILE = "src/input.properties";
	private Map<String, String> emf_structure_datatype;
	private DBProcessing dbpr;
	
	public static void main(String[] args) {

		Main m = new Main();
		m.input = new TreeMap<String, String>();

		// Take user input
		System.out.print("Read the input file...\n");
		m.ReadProperty("src/input.properties");
		System.out.print("Read input file success!\n");

		
		m.DataTypeMapInit(); // function reads the input map and generates the EMFStructure		
		
		try {
			m.Generator(); //function that generates the EMFStructure and QP java files
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace(System.err);
		}
	}
	
	public void DataTypeMapInit() {
		dbpr = new DBProcessing();

		emf_structure_datatype = new TreeMap<String, String>();

		//Process for Grouping attributes if any
		String[] gas = input.get(NameDefine.GROUPING_ATTRIBUTES).split(",");
		
		if (gas.length > 0 && gas[0].length() > 0) {
			for (String ga : gas) {
				String result = dbpr.GetDataType(ga); // get the datatype
				if (result.contains("character"))
					emf_structure_datatype.put(ga, "String");
				else if (result.contains("integer"))
					emf_structure_datatype.put(ga, "int");

			}
		}

		//Process for F_vector, if any
		String[] fvect_atts = input.get(NameDefine.F_VECT).split(",");
		
		if (fvect_atts.length > 0 && fvect_atts[0].length() > 0) {
			for (String att : fvect_atts) {
				String substr = att.substring(0, 3); // get the aggregation function
				if (substr.equalsIgnoreCase("avg")) {	// to get avg(), we need sum() and count()
					String remstr = att.substring(3);
					
					if (!emf_structure_datatype.containsKey("sum" + remstr))	// make sure store only once
						emf_structure_datatype.put("sum" + remstr, "int");
					if (!emf_structure_datatype.containsKey("count" + remstr))
						emf_structure_datatype.put("count" + remstr, "int");
					if (!emf_structure_datatype.containsKey("avg" + remstr))
						emf_structure_datatype.put("avg" + remstr, "double");
				} else {
					if (!emf_structure_datatype.containsKey(att))
						emf_structure_datatype.put(att, "int");
				}
			}
		}
	}
	
	public void ReadProperty(String fpath) {
		Properties prop = new Properties();
		File file = new File(fpath);
		FileInputStream fi;
		try {
			fi = new FileInputStream(file); //create object for reading input file
			prop.load(fi); //load the file to properties obj - this identifies the keys and values from the input
			fi.close(); //close the file obj
			
			//loop for every key in the input file
			Enumeration<Object> enumKeys = prop.keys();
			while (enumKeys.hasMoreElements()) {
				String key = (String) enumKeys.nextElement();
				String value = prop.getProperty(key);
				input.put(key, value); //add the key-value to Map - input
			}
		} catch (FileNotFoundException e) {
			System.out.println("Error in reading input file: " + e.getMessage());
		} catch (IOException e) {
			System.out.println("Fail in IO!");
			e.printStackTrace();
		}
	}

	public void Generator() {
		boolean isCreated = false;
		
		isCreated = FileGenerator.EMFStructureGenerator(emf_structure_datatype); //function to generate the emf-structure output file

		if (isCreated) //condition to check if emf-structure output file is successfully created
			FileGenerator.GenerateQPClass(input, emf_structure_datatype); //function to generate the QP output file
		else
			System.out.println("Error! Creating Structure Fail!");
	}

}
