package com.enterprise.visao360.lake.constantes;

public class VISAO360LakeConstantes {
	public static String CONFIG_MOTOR_CLIENTE                  = "motorcliente";
	public static String CONFIG_MOTOR_CONTA                    = "motorconta";
	public static String TABELA_CLIENTE_PEDT150                = "PEDT150";
	public static String TABELA_CLIENTE_PEDT001                = "PEDT001";
	public static String TABELA_CARTEIRA_VEDT001               = "VEDT001";
	public static String TABELA_CARTEIRA_VEDT002               = "VEDT002";
	public static String TABELA_PRODUTO_TC0111                 = "tc0111";
	public static String PESSOA_FISICA                         = "F";
	public static String PESSOA_JURIDICA                       = "J";
	public static String REPRESENTACAO_PESSOA_FISICA_PEDT001   = "13";
	public static String REPRESENTACAO_PESSOA_JURIDICA_PEDT001 = "14";
	public static String CODIGO_EMPRESA_BANCO                  = "33";
	public static String SIGLA_PRODUTO                         = "DESCONTO";
	public static final String SECURITY_AUTHENTICATION         = "hbase.security.authentication";
	public static final String KERBEROS                        = "kerberos";
	
	public static final String GR = "GR";

	public static final String GA = "GA";

	public static final String GG = "GG";
	// Arquivo de parametrização notificação fim

	// Tabela Gerente inicio
	public static final String CODIGO_UNIDADE = "CodigoUnidade";

	public static final String NOME_GERENTE = "NomeGerente";

	public static final String TIPO_UNIDADE = "TipoUnidade";

	public static final String SIGLA_GERENTE = "SiglaGerente";

	public static final String MATRICULA_GERENTE = "MatriculaGerente";

	public static final String PERFIL_GERENTE = "PerfilGerente";
	
	public static final String GRUPO_PERFIL = "GrupoPerfil";

	public static final String TABLE_NAME_GERENTES = "frontunico:gerentes";
	
	public static final String TABLE_NAME_GERENTES_UNIDADES = "frontunico:gerentesunidade";

	public static final String COLUMN_FAMILY_NAME_G = "g";
	
	public static final String COLUMN_FAMILY_NAME_GU = "gu";

	public static final String[] TABLE_MANAGER = {VISAO360LakeConstantes.NOME_GERENTE,
												  VISAO360LakeConstantes.SIGLA_GERENTE,
												  VISAO360LakeConstantes.MATRICULA_GERENTE,
												  VISAO360LakeConstantes.PERFIL_GERENTE,
												  VISAO360LakeConstantes.CODIGO_UNIDADE,
												  VISAO360LakeConstantes.TIPO_UNIDADE,
												  VISAO360LakeConstantes.GRUPO_PERFIL};
	
	// Tabela Gerente fim

	public static final String AMBIENTE_PADRAO = "dev";

	// Tabela hierarquia inicio
	public static final String COLUMN_FAMILY_NAME_H = "g";

	public static final String CODIGO_UNIDADE_RELACIONADA = "CodigoUnidadeRelacionada";

	public static final String TABLE_NAME_HIERARQUIA = "frontunico:hierarquia";

	public static final String[] TABLE_HIERARQUIA = { VISAO360LakeConstantes.CODIGO_UNIDADE_RELACIONADA };
	// Tabela hierarquia fim
	
	public static final String SEPADOR_VIRGULA = ",";
	
	public static final String SEPADOR_HIFEN = "-";
	
	public static final String SEPADOR_PIPE = "|";
	
	public static final String QUERY_DATA_REF="select dat_ref_carga from %s order by dat_ref_carga desc limit 1";


}