package com.enterprise.visao360.lake.dao;



import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MySQLDAO {

public Connection connection;
	

//Colocar as variaveis de configuração de acesso ao banco de dados em arquivo
	public MySQLDAO(){
		
		try { // Load the JDBC driver 
			String driverName = "org.gjt.mm.mysql.Driver"; 
			// MySQL MM JDBC driver 
			Class.forName(driverName); 
			// Create a connection to the database 
			String serverName = "localhost"; 
			String mydatabase = "mydatabase"; 
			String url = "jdbc:mysql://" + serverName + "/" + mydatabase; 
			// a JDBC url 
			String username = "username"; 
			String password = "password"; 
			connection = DriverManager.getConnection(url, username, password); 
			} catch (ClassNotFoundException e) 
			{ 
				e.printStackTrace();
				
			} catch (SQLException e) 
			{ 
				e.printStackTrace();
				} 
		}
}
