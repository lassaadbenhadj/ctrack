package mifos;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.*;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
public class ctrackClass {
	public static Connection conn;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String logfile="ctrakcycleLog.txt";
		logger(logfile,"Time Start:" + new Date());
		openconnection();
		disable_reports();
		drop_ctrack();
		create_ctrack();
		index_ctrack();
		drop_ctrackcycle();
		create_ctrackcycle();
		index_ctrackcycle();
		drop_ctrack();
		update_business_activities_I();
		update_business_activities_II();
		enable_reports();
		logger(logfile,"Time Finish:" + new Date());
		update_batch_last_run_date_table();
	}

	public static void openconnection() {
		try {
			
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			conn = DriverManager.getConnection("jdbc:mysql://192.168.1.213/mifos?" +
	        "user=root&password=mysql");
			System.out.println("Connection ouverte");
		}
		catch (Exception e){
			 System.err.println("ClassNotFoundException: "
			      +e.getMessage());
		}
	}
	
	public static void disable_reports() {
		try {
			Statement stmt = conn.createStatement();
			
			String SqlStr = "UPDATE report set REPORT_ACTIVE=0 WHERE REPORT_ID IN (5,14,42,28,29,19,24,31,40)";
			stmt.executeUpdate(SqlStr);
			System.out.println("Query disable_reports executed with success");
			
			}
			catch(Exception e){
				System.err.println("ClassNotFoundException: "
					      +e.getMessage());
			}
	}
	
	public static void drop_ctrack() {
		try {
			Statement stmt = conn.createStatement();
			
			String SqlStr = "DROP TABLE IF EXISTS ctrack";
			stmt.executeUpdate(SqlStr);
			System.out.println("Query drop_ctrack executed with success");
			
			}
			catch(Exception e){
				System.err.println("ClassNotFoundException: "
					      +e.getMessage());
			}
	}
	
	public static void create_ctrack() {
		try {
			Statement stmt = conn.createStatement();
			
			String SqlStr = " CREATE TABLE ctrack SELECT loan_account.ACCOUNT_ID," +
			" loan_account.ACCOUNT_ID as child_account_id , account.CUSTOMER_ID," +
			" loan_account.BUSINESS_ACTIVITIES_ID, loan_account.DISBURSEMENT_DATE, " +
			" loan_account.LOAN_AMOUNT, loan_account.NO_OF_INSTALLMENTS, ACCOUNT_TYPE_ID, CLOSED_DATE," +
			" loan_account.prd_offering_id, ACCOUNT_STATE_ID, office_id, personnel_id, " +
			" loan_account.fund_id" +
			" FROM (loan_account LEFT JOIN account ON loan_account.ACCOUNT_ID = account.ACCOUNT_ID) " +
			" LEFT JOIN customer ON account.CUSTOMER_ID = customer.CUSTOMER_ID" +
			" WHERE (((account.ACCOUNT_TYPE_ID) = 1) And ((customer.CUSTOMER_LEVEL_ID) = 1))" +
			" UNION SELECT loan_account.PARENT_ACCOUNT_ID, loan_account.ACCOUNT_ID as child_account_id," +
			" account.CUSTOMER_ID, loan_account.BUSINESS_ACTIVITIES_ID, loan_account_1.DISBURSEMENT_DATE," +
			" loan_account.LOAN_AMOUNT, loan_account_1.NO_OF_INSTALLMENTS, account.ACCOUNT_TYPE_ID," +
			" account_parent.CLOSED_DATE , loan_account_1.prd_offering_id, account_parent.ACCOUNT_STATE_ID," +
			" account_parent.OFFICE_ID, account_parent.personnel_id," +
			" loan_account_1.fund_id" +
			" FROM (loan_account LEFT JOIN account ON loan_account.ACCOUNT_ID = account.ACCOUNT_ID)" +
			" LEFT JOIN loan_account AS loan_account_1 ON loan_account.PARENT_ACCOUNT_ID = loan_account_1.ACCOUNT_ID" +
			" LEFT JOIN account as account_parent ON loan_account_1.ACCOUNT_ID = account_parent.ACCOUNT_ID" +
			" WHERE (((account.ACCOUNT_TYPE_ID)=4))";
			stmt.executeUpdate(SqlStr);
			System.out.println("Query create_ctrack executed with success");
			
			}
			catch(Exception e){
				System.err.println("ClassNotFoundException: "
					      +e.getMessage());
			}
			
	}
	
	public static void index_ctrack(){
		try {
			Statement stmt = conn.createStatement();
			
			String SqlStr = " ALTER TABLE `mifos`.`ctrack`  ADD INDEX Index_1 USING BTREE" +
							"(`CUSTOMER_ID`, `DISBURSEMENT_DATE`, `ACCOUNT_STATE_ID`, `ACCOUNT_ID`)";
			stmt.executeUpdate(SqlStr);
			System.out.println("Query index_ctrack executed with success");
			
			}
			catch(Exception e){
				System.err.println("ClassNotFoundException: "
					      +e.getMessage());
			}
		
	}
	
	public static void drop_ctrackcycle(){
		try {
			Statement stmt = conn.createStatement();
			
			String SqlStr = "DROP TABLE IF EXISTS ctrackcycle";
			stmt.executeUpdate(SqlStr);
			System.out.println("Query drop_ctrackcycle executed with success");
			
			}
			catch(Exception e){
				System.err.println("ClassNotFoundException: "
					      +e.getMessage());
			}
		
	}
	
	public static void create_ctrackcycle(){
		try {
			Statement stmt = conn.createStatement();
			
			String SqlStr = " CREATE TABLE ctrackcycle SELECT *, (select count(account_id) as c from ctrack t" +
			" where t.customer_id= ctrack.customer_id and t.disbursement_date<=ctrack.disbursement_date and" +
			" t.ACCOUNT_STATE_ID between 5 and 9) AS Cycle FROM ctrack " ;
			stmt.executeUpdate(SqlStr);
			System.out.println("Query create_ctrackcycle executed with success");
			
			}
			catch(Exception e){
				System.err.println("ClassNotFoundException: "
					      +e.getMessage());
			}
		
	}
	
	public static void index_ctrackcycle(){
		try {
			Statement stmt = conn.createStatement();
			
			String SqlStr = " ALTER TABLE ctrackcycle ADD INDEX Index_1(ACCOUNT_ID)," +
							" ADD INDEX Index_2(CUSTOMER_ID)," +
							" ADD INDEX Index_3(DISBURSEMENT_DATE)," +
							" ADD INDEX Index_4(Cycle)";
			stmt.executeUpdate(SqlStr);
			System.out.println("Query index_ctrackcycle executed with success");
			
			}
			catch(Exception e){
				System.err.println("ClassNotFoundException: "
					      +e.getMessage());
			}
		
	}
	
	public static void enable_reports() {
		try {
			Statement stmt = conn.createStatement();
			
			String SqlStr = "UPDATE report set REPORT_ACTIVE=1 WHERE REPORT_ID IN (5,14,42,28,29,19,24,31,40)";
			stmt.executeUpdate(SqlStr);
			System.out.println("Query enable_reports executed with success");
			
			}
			catch(Exception e){
				System.err.println("ClassNotFoundException: "
					      +e.getMessage());
			}
	}
	
	private static void logger(String destination,String mms) {  

		try { 

			FileWriter fstream = new FileWriter(destination,true);
		    BufferedWriter out = new BufferedWriter(fstream);
		    out.write(mms);
		    out.newLine();
		    //Close the output stream
		    out.close();

		} catch (Exception e) { 
    
			e.printStackTrace(); 
   
			}      
  
	}    
	
	public static void update_batch_last_run_date_table() {
		try {
			DateFormat mysqldateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String formateddate=mysqldateFormat.format(new Date());
			
			Statement stmt = conn.createStatement();
			
			String SqlStr = "UPDATE batch_last_run_date set last_run_date='" + formateddate + "' WHERE batch_id=1";
			stmt.executeUpdate(SqlStr);
			System.out.println("Query update_batch_last_run_date_table executed with success");
			
			}
			catch(Exception e){
				System.err.println("ClassNotFoundException: "
					      +e.getMessage());
			}
	}
	
	public static void update_business_activities_I() {
		try {
			
			Statement stmt = conn.createStatement();
			
			String SqlStr = "UPDATE ctrackcycle JOIN (SELECT  entity_id,tag AS 'Activity'," +
				   " 1,customer_id" +
				   " FROM question_group_instance join question_group_response" +
				   " ON question_group_instance.id=question_group_response.question_group_instance_id" +
				   " JOIN ctrackcycle ON ctrackcycle.account_id=question_group_instance.entity_id" +
				   " Where question_group_id = 19" +
				   " AND sections_questions_id = 286" +
				   " GROUP BY entity_id" +
				   " union" +
				   " SELECT  entity_id,tag AS 'Activity'," +
				   " 1,customer_id" +
				   " FROM question_group_instance join question_group_response" +
				   " ON question_group_instance.id=question_group_response.question_group_instance_id" +
				   " JOIN ctrackcycle ON ctrackcycle.account_id=question_group_instance.entity_id" +
				   " Where question_group_id = 20" +
				   " AND sections_questions_id = 297" +
				   " GROUP BY entity_id" +
				   " union" +
				   " SELECT  entity_id,tag AS 'Activity'," +
				   " 1,customer_id" +
				   " FROM question_group_instance join question_group_response" +
				   " ON question_group_instance.id=question_group_response.question_group_instance_id" +
				   " JOIN ctrackcycle ON ctrackcycle.account_id=question_group_instance.entity_id" +
				   " Where question_group_id = 22" +
				   " AND sections_questions_id = 317" +
				   " GROUP BY entity_id) as Activ" +
				   " on entity_id=account_id and Activ.customer_id=ctrackcycle.customer_id" +
				   " join question_business_activities_sector on question_business_activities_sector.business_activity_name=Activ.Activity" +
				   " SET ctrackcycle.business_activities_id=question_business_activities_sector.business_activity_id";
			
			stmt.executeUpdate(SqlStr);
			System.out.println("Query update_business_activities executed with success");
			
			}
			catch(Exception e){
				System.err.println("ClassNotFoundException: "
					      +e.getMessage());
			}
	}
	
	public static void update_business_activities_II() {
		try {
			/* add id for consumption activities*/
			Statement stmt = conn.createStatement();
			
			String SqlStr = "update ctrackcycle JOIN prd_offering USING (PRD_OFFERING_ID)" +
							" set business_activities_id=82" +
							" where disbursement_date>='2012-05-01' AND PRD_CATEGORY_ID in (4,9)";
			
			stmt.executeUpdate(SqlStr);
			System.out.println("Query update_business_activities_II executed with success");
			
			}
			catch(Exception e){
				System.err.println("ClassNotFoundException: "
					      +e.getMessage());
			}
	}
}
