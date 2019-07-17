package com.enterprise.visao360.lake.utils;

import com.enterprise.visao360.lake.enums.AmbienteEnum;
import com.enterprise.visao360.lake.exception.MotorEnriquecimentoArgumentException;

//import com.enterprise.visao360.lake.enums.MotoresEnriquecimentoEnum;

public class ArgumentValidationUtil {
	
	public static Boolean validarArgumentos(String[] args) throws MotorEnriquecimentoArgumentException {
				
		if(args == null || args.length == 0) {
			return false;
		}
		
		//Validar Primeiro Argumento - Motor de Enriquecimento Acionado
		if (args[0].isEmpty() || !args[0].getClass().equals(String.class)) {
			throw new MotorEnriquecimentoArgumentException("motorEnriquecimento", "argumento vazio ou incompativel");
		}
		
			
		//Validar Segundo Parametro - Ambiente
		if (args[1].isEmpty() || !args[1].getClass().equals(String.class)) {
			throw new MotorEnriquecimentoArgumentException("ambiente", "argumento vazio ou incompativel");
		}
		
		if(!AmbienteEnum.isValid(args[1]))
		{			
			throw new MotorEnriquecimentoArgumentException("ambiente", "Ambiente [" + args[1] + "] inválido. Os ambientes validos são:" + AmbienteEnum.toList());
		}
		
		return true;
		
	}

}
