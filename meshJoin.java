package dwh;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.Month;
import java.time.temporal.IsoFields;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.sql.*;
import org.javatuples.Quintet;
import org.javatuples.Ennead;



public class dwhh {
	

	
	@SuppressWarnings("null")
	static List<List<Object>> DiskBufferGenerator(ResultSet s) throws SQLException {
		List<List<Object>> listOfLists= new ArrayList<List<Object> >();
		
		while(s.next())
		{
			
			Quintet<String, String, String, String, Float> quintet =Quintet.with(s.getString(1),s.getString(2),s.getString(3),s.getString(4),s.getFloat(5));
			List<Object> tempList = quintet.toList();
			listOfLists.add(tempList);
		}
		
		
		return listOfLists;
		
	}
	
	@SuppressWarnings("null")
	static List<List<Object>> StreamBufferGenerator(ResultSet s) throws SQLException {
		List<List<Object>> listOfLists= new ArrayList<List<Object> >();
		
		while(s.next())
		{
			
			Ennead<Integer, String, String, String, String, String, String,String,Float> ennead = Ennead.with(s.getInt(1),s.getString(2),s.getString(3),s.getString(4)
					,s.getString(5),s.getString(6),s.getString(7),s.getString(8),s.getFloat(9));
			List<Object> nintet = ennead.toList();
			listOfLists.add(nintet);
		}
		
		return listOfLists;
		
	}
	
	public static Boolean IterativeCall(
			  Object object, List<List<Object>> elements) {

			    for (List<Object> elem : elements) {
			        if (elem.get(0).equals(object)) {
			            return true;
			        }
			    }
			    return false;
			}
			
	
	public static List<Object> Formatted_Date(String date)
	{
		
		
		// Get an instance of LocalTime
		// from date
		LocalDate currentDate
			= LocalDate.parse(date);
		// Get day from date
		int day = currentDate.getDayOfMonth();

		//Get Week from date
		DayOfWeek day1 = currentDate.getDayOfWeek();
		
		// Get month from date
		Month month = currentDate.getMonth();
		
		//quarter from date
		int quarter = currentDate.get(IsoFields.QUARTER_OF_YEAR);
		// Get year from date
		int year = currentDate.getYear();
		
		Quintet<Integer, String, String, Integer, Integer> quintet =Quintet.with(day,day1.toString(),month.toString(),quarter,year);
		List<Object> quintetList = quintet.toList();
		
		return quintetList;
	}

	
	public static void main(String[] args) throws SQLException {
		Connection conn=DriverManager.getConnection("jdbc:mysql://localhost:3306/db","root","adan123");
		Statement stmt=conn.createStatement();
		Connection con1=DriverManager.getConnection("jdbc:mysql://localhost:3306/db","root","adan123");
		Statement stmt1=con1.createStatement();
		
		Connection con3=DriverManager.getConnection("jdbc:mysql://localhost:3306/starschema","root","adan123");
		Statement stmt3=con3.createStatement();
		
		// TODO Auto-generated method stub
		
		//Disk Buffer
		ArrayList<List<List<Object>>> DiskBuffers = new ArrayList<List<List<Object>>>();
		for(int i=0;i<10;i++)
		{
		
			ResultSet s = stmt.executeQuery("select * from products limit "+i*10+","+(i+1)*10);
			List<List<Object>> DiskList=DiskBufferGenerator(s);
			DiskBuffers.add(DiskList);
		}
		
		
		int count=0;
		int count1=0;
		List<List<Object>> StreamQueue =new ArrayList<List<Object>>();
		
		for(int i=0;i<200;i++)
		{
			//count++;
			ResultSet s = stmt.executeQuery("select * from transactions_data limit "+i*50+","+(i+1)*50);
			//Convert to list
			List<List<Object>> StreamList=StreamBufferGenerator(s);
			//Mesh Join
		

			for(List<List<Object>> DiskBuffer:DiskBuffers)
			{
				for(List<Object> b:DiskBuffer)
				{
					for(List<Object> s1:StreamList)
					{
						if(Objects.equals(b.get(0),s1.get(1)))
		
						{
							if(!IterativeCall(s1.get(0),StreamQueue)) {
							 
								count1+=1;
								System.out.println(count+" Rows Inserted Into Warehouse.");
								
								List<Object> newList = new ArrayList<Object>();
								for (int val=0;val<8;val++) {
								newList.add(s1.get(val));
								}
		
								count++;
								for (int val=1;val<4;val++) {
								newList.add(b.get(val));
								
								}
								Float TotalSale=(Float)b.get(4)*(Float)s1.get(8);
								newList.add(TotalSale);
								
		
								
								String product_dim="Insert Ignore Into Product(product_id,product_name) values (?,?)";
						
								PreparedStatement preparedStatement = con3.prepareStatement(product_dim);
								int trigger=1;
								for (int val=0;val<2;val++) {
								preparedStatement.setString(val+1, (String) newList.get(trigger));
								trigger+=7;
								}
								preparedStatement.execute();
								
			
								
								String customer_dim="Insert Ignore Into customer(customer_id,customer_name) values (?,?)";
								PreparedStatement preparedStatement1 = con3.prepareStatement(customer_dim);
								
								preparedStatement1.setString(1, (String) newList.get(2));
								preparedStatement1.setString(2,(String) newList.get(3));
				
								preparedStatement1.execute();
				
								
								String store_dim="Insert Ignore Into store(store_id,store_name) values (?,?)";
								PreparedStatement preparedStatement2 = con3.prepareStatement(store_dim);
								
								
								preparedStatement2.setString(1, (String) newList.get(4));
								preparedStatement2.setString(2, (String) newList.get(5));
							
	
					
								
								preparedStatement2.execute();
		
								
								String supplier_dim="Insert Ignore Into supplier(supplier_id,supplier_name) values (?,?)";
								PreparedStatement preparedStatement4 = con3.prepareStatement(supplier_dim);
								
							
								preparedStatement4.setString(1, (String) newList.get(9));
								preparedStatement4.setString(2, (String) newList.get(10));
								
	
								preparedStatement4.execute();
								
								List<Object> DateList=Formatted_Date((String)newList.get(7));
								
								
								String date_dim="Insert Ignore Into t_date(time_id,Day,Week,Month,quarterly,yearly) values (?,?,?,?,?,?)";
								PreparedStatement preparedStatement5 = con3.prepareStatement(date_dim);
								
								
								preparedStatement5.setString(1, (String) newList.get(6));
								preparedStatement5.setLong(2, (Integer) DateList.get(0));
								preparedStatement5.setString(3, (String) DateList.get(1));
								preparedStatement5.setString(4, (String) DateList.get(2));
								preparedStatement5.setLong(5, (Integer) DateList.get(3));
								preparedStatement5.setLong(6, (Integer) DateList.get(4));
								preparedStatement5.execute();
								
								/*for (int temp=0;temp<12;temp++) {
									System.out.println(newList.get(temp));
								}*/
								//System.out.println("hello");
								//String fact_metroSales="Insert Ignore Into metro_sales (transaction_id,product_id,customer_id,store_id,supplier_id,date_id,total_sale) values (?,?,?,?,?,?,?)";
						
								String Product6="Insert Ignore Into metro_sales (Transaction_ID,PRODUCT_ID,CUSTOMER_ID,STORE_ID,SUPPLIER_ID,time_id,Total_Sale) values (?,?,?,?,?,?,?)";
								PreparedStatement preparedStatement6 = con3.prepareStatement(Product6);
								preparedStatement6.setLong(1, (Integer) newList.get(0));
								preparedStatement6.setString(2, (String) newList.get(1));
								preparedStatement6.setString(3, (String) newList.get(2));
								preparedStatement6.setString(4, (String) newList.get(4));
								
								preparedStatement6.setString(5, (String) newList.get(9));
								preparedStatement6.setString(6, (String) newList.get(6));
								preparedStatement6.setFloat(7, (Float) newList.get(11));
								preparedStatement6.execute();
								
								
								StreamQueue.add(newList);
								
								//for (int temp=0;temp<12;temp++) {
									//Syztem.out.println(newList.get(temp));
								//}
						}
							
					

	}

}
}
			}
	}
		System.out.println("Insertion Successfull!");
		System.out.print(count1);
	}
}
