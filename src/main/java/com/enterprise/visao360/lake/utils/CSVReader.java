package com.enterprise.visao360.lake.utils;
/*package com.enterprise.visao360.lake.utils;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

public class CSVReader {	

	private String pathParametro;// = "src/main/resources/depara-grupoperfis.csv";
	
	public CSVReader(){}
	
	public CSVReader(String pathParametro){
		this.pathParametro = pathParametro;
	}

	*//**
	 * 
	 * Método para obter a parametrização de perfil
	 * 
	 * @return List<Profile>
	 * **//*
	public List<Profile> getProfile(){
		    String line = "";
	        String cvsSplitBy = ";";

	        boolean firstLine = true;
	        
	        List<Profile> list = new ArrayList<Profile>();
	        
	        Profile profile = null;
	        	        
	        try (BufferedReader br = new BufferedReader(new FileReader(pathParametro))) {

	            while ((line = br.readLine()) != null) {
	            	
	            	if(firstLine){
	            		firstLine = false;
	            		continue;
	            	}
	            	
	            	profile= new Profile();

	                // use dot and comma as separator
	                String[] result = line.split(cvsSplitBy);
	                
	                profile = new Profile();
					profile.setProfile(result[0]);
					profile.setDescription(result[1]);
					profile.setAgregation(result[2]);
					
	                list.add(profile);

	            }

	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	        
	        return list;
	}
	
	*//**
	 * 
	 * Parametrização de Relacionamento entre Unidade Comercial e Unidade Operacional.
	 * 
	 * @return List<Comercial>
	 * **//*
	public List<Comercial> getComercialOperacional(){
		    String line = "";
	        String cvsSplitBy = ";";

	        boolean firstLine = true;
	        
	        List<Comercial> list = new ArrayList<Comercial>();
	        
	        Comercial comercial = null;
	        	        
	        try (BufferedReader br = new BufferedReader(new FileReader(pathParametro))) {

	            while ((line = br.readLine()) != null) {
	            	
	            	if(firstLine){
	            		firstLine = false;
	            		continue;
	            	}
	            	
	            	comercial = new Comercial();

	                String[] result = line.split(cvsSplitBy);
	                
	                comercial = new Comercial();
					comercial.setComercialUniOrgType(result[0]);
					comercial.setComercialUniOrgCode(result[1]);
					comercial.setOperationUniOrgType(result[2]);
					comercial.setOperationUniOrgCode(result[3]);
					comercial.setComercialUniOrgName(result[4]);
					comercial.setNetWork(result[5]);
					comercial.setRegion(result[6]);
					
	                list.add(comercial);

	            }

	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	        
	        return list;
	}
	
	public List<TipoCarteira> getTipoCarteira(List<String[]> results) {

		List<TipoCarteira> list = new ArrayList<TipoCarteira>();
		try {
			
			for (String[] result : results) {
				
					
			TipoCarteira tipoCarteira = new TipoCarteira();
			tipoCarteira.setGrupoTratamento(result[0]);
			tipoCarteira.setTipoCarteira(result[1]);
			tipoCarteira.setPrioridade(Convert.toInteger(result[3]));
			tipoCarteira.setTipoCarteiraDigitalAssociada(result[3]);
			tipoCarteira.setTipoLocalCarteira(result[2]);
			
			list.add(tipoCarteira);
			}
			return list;

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	

	public List<String[]> obterResultArquivo() throws IOException {
		
		
		List<String[]> resultItems = new ArrayList<String[]>();
		String line = "";
		String cvsSplitBy = ";";

		boolean firstLine = true;
		String[] result = new String[] {};
		
		try (BufferedReader br = new BufferedReader(new FileReader(pathParametro))) {

			while ((line = br.readLine()) != null) {

				if (firstLine) {
					firstLine = false;
					continue;
				}
				// use dot and comma as separator
				result = line.split(cvsSplitBy);
				resultItems.add(result);
								

			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return resultItems;

	}

}
*/