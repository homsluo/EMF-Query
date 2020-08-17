package EMFSQL;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.util.Properties;
public class PropertyGenerator {
	public static void main( String[] args ) throws Exception
	{
		Properties p = new Properties();
		OutputStream os = new FileOutputStream("src/input.properties");
		
		p.setProperty("select_attributes", "prod,month,emftable[index].sum_quant_1 * 1.0/emftable[index].sum_quant_2");
		p.setProperty("no_of_grp_var", "2");
		p.setProperty("grouping_attributes", "prod,month");
		p.setProperty("f_vect", "sum_quant_1,sum_quant_2");
		p.setProperty("select_condition_vect", "emftable[index].prod.equals(prod) && emftable[index].month == month, emftable[index].prod.equals(prod)");
		p.setProperty("having_condition", "");
		
		p.store(os,  null);
		System.out.println("Success!");
	}
}
