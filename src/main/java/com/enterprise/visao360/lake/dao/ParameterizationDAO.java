package com.enterprise.visao360.lake.dao;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ParameterizationDAO extends MySQLDAO{
	
	
	//Arrumar a query para quando a tabela for criada!!!!
	public String getManagerType(String manager){	
		String result = "";
		try{
			
			String sql = "select * from parametrizacao where perfil='"+manager+"'";
			
			Statement stm = connection.createStatement();
			
			ResultSet rs = stm.executeQuery(sql);
			
			while(rs.next()){

				result = rs.getString("reduce");
				
			}
			
			connection.close();

		}catch(SQLException e){
			e.printStackTrace();
			
		}	
		
		return result;
	}	
	
	

}
