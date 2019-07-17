package com.enterprise.visao360.lake.business; /**package com.enterprise.frontunico.lake.business;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import com.enterprise.frontunico.lake.config.TableConfiguration;
import com.enterprise.frontunico.lake.constantes.FrontUnicoLakeConstantes;
import com.enterprise.frontunico.lake.dao.HBaseDAO;
import com.enterprise.frontunico.lake.model.Comercial;
import com.enterprise.frontunico.lake.model.Hierarquia;
import com.enterprise.frontunico.lake.model.Manager;
import com.enterprise.frontunico.lake.model.T0004;
import com.enterprise.frontunico.lake.utils.CSVReader;
import com.enterprise.frontunico.lake.utils.Parse;
import com.enterprise.frontunico.lake.utils.Utils;
import com.enterprise.idl.frontunico.dao.enriquecimento.HierarquiaDao;
import com.enterprise.idl.frontunico.model.HierarquiaEntity;

public class MotorEnriquecimentoHierarquiaBusinessImpl implements MotorEnriquecimentoHierarquiaBusiness {
	
	private static final String COD_HIERARQUIA_0001 = "0001";
	private static final String COD_HIERARQUIA_GERENCIAL = "0038";
	private static final String COD_UNIDADE_SUBORDINADA = "0015";
	final static Logger LOGGER = Logger.getLogger(MotorEnriquecimentoHierarquiaBusinessImpl.class);
	
	@Override
	public void inserirDadosHierarquiaHazealCast(final List<Manager> managerList,
			final TableConfiguration tableConfiguration) throws Exception {
		LOGGER.info("**********************INICIANDO METODO inserirDadosHierarquiaHazealCast********************");
		final HierarquiaDao hierarquiaDao = new HierarquiaDao();
		LOGGER.info("managerList: " +managerList);
		try {
			HierarquiaEntity hierarquiaEntity = null;
			final Parse parse = new Parse();
			for (Manager manager : managerList) {
				hierarquiaEntity = parse.parseManagerToHierarquiaEntity(manager);
				hierarquiaDao.insert(hierarquiaEntity);
				LOGGER.info("Dados inseridos no hazeal: " +hierarquiaEntity);
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		} finally {
			hierarquiaDao.closeConnection();
			LOGGER.info(
					"**********************FINALIZANDO METODO inserirDadosHierarquiaHazealCast********************");
		}
	}
	
	/**
	 * @param result
	 * @param tableConfiguration
	 * @return the void
	 *//*
	public void inserirDadosHierarquiaHbase(final List<Manager> managers,
			final TableConfiguration tableConfiguration) {
		LOGGER.info("**********************INICIANDO METODO inserirDadosHierarquiaHbase**********************");
		try {
			final HBaseDAO hBaseDAO = new HBaseDAO();
			hBaseDAO.hbaseConfig(tableConfiguration.getPathHbaseSite(), tableConfiguration.getPathKeyTab(),
					tableConfiguration.getUserKeyTab());
			
			if(managers != null) {
				for (Manager manager : managers) {
					final Map<String, String> paramValues = new HashMap<String, String>();
					paramValues.put("Unidade", manager.getCodigoUnidade());
					paramValues.put("TipoUnidade", manager.getTipoUnidade());
					paramValues.put("Matricula", manager.getMatriculaGerente());
					paramValues.put("GrupoPerfil", manager.getGrupoPerfil());
					paramValues.put("Perfil", manager.getPerfilGerente());
					paramValues.put("NomeUsuario", manager.getNome());
					paramValues.put("UsuarioAcesso", manager.getSiglaGerente());
					
					String key = Utils.generateKey(FrontUnicoLakeConstantes.SEPADOR_HIFEN, manager.getTipoUnidade(),
							manager.getCodigoUnidade(), manager.getMatriculaGerente());
					
					hBaseDAO.put(tableConfiguration.getTableName(),
							key, tableConfiguration.getDataKey(), paramValues);
					
					LOGGER.info("Dados inseridos no Hbase: key=" +key+ ", paramValues=" +paramValues);
				}
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		} finally {
			LOGGER.info("**********************FINALIZANDO METODO inserirDadosHierarquiaHbase**********************");
		}
	}
	
	private static List<Manager> searchManager(final String rowKey, final String tableName, final String columnFamily,
			final String[] columns, final HBaseDAO hDAO) throws IOException {
		
		LOGGER.info("**********************INICIANDO METODO searchManager**********************");
		
		LOGGER.info("******rowKey*******" + rowKey + "*************");
		LOGGER.info("******tableName*******" + tableName + "*************");
		LOGGER.info("******columnFamily*******" + columnFamily + "*************");
		LOGGER.info("******columns*******" + Arrays.toString(columns) + "*************");
		List<Result> listResult = hDAO.scan(rowKey, tableName, columnFamily, columns);
		List<Manager> managerList = null;
		if (listResult != null && !listResult.isEmpty()) {
			managerList = new ArrayList<Manager>();
			final Parse parse = new Parse();
			for (Result res : listResult) {
				final Manager manager = parse.parseManager(res);
				managerList.add(manager);
			}
		}
		
		LOGGER.info("result: " +managerList);
		
		LOGGER.info("**********************FINALIZANDO METODO searchManager**********************");
		
		return managerList;
	}
	public List<Manager> searchManagerRedeAgencia(final List<Hierarquia> listaHierarquia,
			final TableConfiguration tableConfiguration, final DataFrame t0004DF) throws IOException {
		
		LOGGER.info("**********************INICIANDO METODO searchManagerRedeAgencia**********************");
		final HBaseDAO hDAO = new HBaseDAO();
		hDAO.hbaseConfig(tableConfiguration.getPathHbaseSite(), tableConfiguration.getPathKeyTab(),
				tableConfiguration.getUserKeyTab());
		final List<Manager> managerListGG = searchManagerGG(listaHierarquia, tableConfiguration, hDAO, t0004DF);
		final List<Manager> managerListGA = searchManagerGA(listaHierarquia, tableConfiguration, hDAO, t0004DF);
		final List<Manager> managerList = new ArrayList<Manager>();
		
		if(CollectionUtils.isNotEmpty(managerListGA)) {
			managerList.addAll(managerListGA);
		}
		
		if(CollectionUtils.isNotEmpty(managerListGG)) {
			managerList.addAll(managerListGG);
		}
		
		LOGGER.info("**********************FINALIZANDO METODO searchManagerRedeAgencia**********************");
		
		return managerList;
	}
	private static List<Manager> searchManagerGG(final List<Hierarquia> listaHierarquia,
			final TableConfiguration tableConfiguration, final HBaseDAO hDAO, final DataFrame t0004DF)
			throws IOException {
		LOGGER.info("**********************INICIANDO METODO searchManagerGG**********************");
		List<Manager> listManagerGG = null;
		List<Manager> listManager = null;
		List<Manager> listManagerResult = new ArrayList<Manager>();
		for (final Hierarquia unidadePonteiro : listaHierarquia) {
			
			LOGGER.info("Unidade Ponteiro: " +unidadePonteiro);
			listManager = searchManager(
					Utils.generateKey(FrontUnicoLakeConstantes.SEPADOR_HIFEN, Utils.leftPad(unidadePonteiro.getTipoUnidade()),
							unidadePonteiro.getUnidade()),
					FrontUnicoLakeConstantes.TABLE_NAME_GERENTES_UNIDADES, FrontUnicoLakeConstantes.COLUMN_FAMILY_NAME_GU,
					FrontUnicoLakeConstantes.TABLE_MANAGER, hDAO);
			listManagerGG = verifyManagerProfile(FrontUnicoLakeConstantes.GG, listManager);
			if (CollectionUtils.isEmpty(listManagerGG)) {
				T0004 t0004 = searchUpperUnit(unidadePonteiro.getTipoUnidade(), unidadePonteiro.getUnidade(), COD_HIERARQUIA_GERENCIAL,
						t0004DF);
				
				if(t0004 != null) {
					listManager = searchManager(
							Utils.generateKey(FrontUnicoLakeConstantes.SEPADOR_HIFEN, Utils.leftPad(t0004.getTipoUnidadeSuperior()),
									t0004.getCodigoUnidadeSuperior()),
							FrontUnicoLakeConstantes.TABLE_NAME_GERENTES_UNIDADES, FrontUnicoLakeConstantes.COLUMN_FAMILY_NAME_GU,
							FrontUnicoLakeConstantes.TABLE_MANAGER, hDAO);
					listManagerGG = verifyManagerProfile(FrontUnicoLakeConstantes.GG, listManager);
				}
			}
			listManagerGG = alterUnityManagers(unidadePonteiro.getUnidade(), unidadePonteiro.getTipoUnidade(),
					listManagerGG);
			
			if(CollectionUtils.isNotEmpty(listManagerGG)) {
				listManagerResult.addAll(listManagerGG);
			}
		}
		
		LOGGER.info("listManagerGG: " +listManagerResult);
		LOGGER.info("**********************FINALIZANDO METODO searchManagerGG**********************");
		return listManagerResult;
	}
	private static List<Manager> searchManagerGA(final List<Hierarquia> listaHierarquia,
			final TableConfiguration tableConfiguration, final HBaseDAO hDAO, final DataFrame t0004DF)
			throws IOException {
		LOGGER.info("**********************INICIANDO METODO searchManagerGA**********************");
		List<Manager> listManagerGA = null;
		List<Manager> listManager = null;
		List<Manager> listManagerResult = new ArrayList<Manager>();
		T0004 t0004 = null;
		for (final Hierarquia unidadePonteiro : listaHierarquia) {
			
			LOGGER.info("Unidade Ponteiro: " +unidadePonteiro);
			listManager = searchManager(
					Utils.generateKey(FrontUnicoLakeConstantes.SEPADOR_HIFEN, Utils.leftPad(unidadePonteiro.getTipoUnidade()),
							unidadePonteiro.getUnidade()),
					FrontUnicoLakeConstantes.TABLE_NAME_GERENTES_UNIDADES, FrontUnicoLakeConstantes.COLUMN_FAMILY_NAME_GU,
					FrontUnicoLakeConstantes.TABLE_MANAGER, hDAO);
			listManagerGA = verifyManagerProfile(FrontUnicoLakeConstantes.GA, listManager);
			if (CollectionUtils.isEmpty(listManagerGA)) {
				LOGGER.info("**********************BUSCANDO UNIDADE INFERIOR**********************");
				t0004 = searchLowerUnit(unidadePonteiro.getTipoUnidade(), unidadePonteiro.getUnidade(), COD_HIERARQUIA_0001,
						COD_UNIDADE_SUBORDINADA, t0004DF);
				
				if(t0004 != null) {
					listManager = searchManager(
							Utils.generateKey(FrontUnicoLakeConstantes.SEPADOR_HIFEN,
									Utils.leftPad(t0004.getTipoUnidadeInferior()), t0004.getCodigoUnidadeInferior()),
							FrontUnicoLakeConstantes.TABLE_NAME_GERENTES_UNIDADES, FrontUnicoLakeConstantes.COLUMN_FAMILY_NAME_GU,
							FrontUnicoLakeConstantes.TABLE_MANAGER, hDAO);
					listManagerGA = verifyManagerProfile(FrontUnicoLakeConstantes.GA, listManager);
				}
				if (CollectionUtils.isEmpty(listManagerGA)) {
					LOGGER.info("**********************BUSCANDO UNIDADE SUPERIOR**********************");
					t0004 = searchUpperUnit(unidadePonteiro.getTipoUnidade(), unidadePonteiro.getUnidade(), COD_HIERARQUIA_GERENCIAL,
							t0004DF);
					
					if(t0004 != null) {
						listManager = searchManager(
								Utils.generateKey(FrontUnicoLakeConstantes.SEPADOR_HIFEN, Utils.leftPad(t0004.getTipoUnidadeSuperior()),
										t0004.getCodigoUnidadeSuperior()),
								FrontUnicoLakeConstantes.TABLE_NAME_GERENTES_UNIDADES, FrontUnicoLakeConstantes.COLUMN_FAMILY_NAME_GU,
								FrontUnicoLakeConstantes.TABLE_MANAGER, hDAO);
						listManagerGA = verifyManagerProfile(FrontUnicoLakeConstantes.GA, listManager);
						
						if (CollectionUtils.isEmpty(listManagerGA)) {
							LOGGER.info("**********************BUSCANDO UNIDADE INFERIOR DA SUPERIOR**********************");
							t0004 = searchLowerUnit(t0004.getTipoUnidadeSuperior(), t0004.getCodigoUnidadeSuperior(),
									COD_HIERARQUIA_0001, COD_UNIDADE_SUBORDINADA, t0004DF);
							
							if(t0004 != null) {
								listManager = searchManager(
										Utils.generateKey(FrontUnicoLakeConstantes.SEPADOR_HIFEN, Utils.leftPad(t0004.getTipoUnidadeInferior()),
												t0004.getCodigoUnidadeInferior()),
										FrontUnicoLakeConstantes.TABLE_NAME_GERENTES_UNIDADES,
										FrontUnicoLakeConstantes.COLUMN_FAMILY_NAME_GU, FrontUnicoLakeConstantes.TABLE_MANAGER,
										hDAO);
								listManagerGA = verifyManagerProfile(FrontUnicoLakeConstantes.GA, listManager);
								
							}
						}
					}
				}
			}
			listManagerGA = alterUnityManagers(unidadePonteiro.getUnidade(), unidadePonteiro.getTipoUnidade(),
					listManagerGA);
			
			if(CollectionUtils.isNotEmpty(listManagerGA)) {
				listManagerResult.addAll(listManagerGA);
			}
		}
		
		LOGGER.info("listManagerGA: " +listManagerGA);
		LOGGER.info("**********************FINALIZANDO METODO searchManagerGA**********************");
		return listManagerResult;
	}
	
	private static List<Manager> alterUnityManagers(final String codigoUnidade, final String tipoUnidade,
			final List<Manager> managers) throws IOException {
		
		LOGGER.info("**********************INICIANDO METODO alterUnityManagers**********************");
		
		if(managers != null) {
			for (Manager manager : managers) {
				manager.setCodigoUnidade(codigoUnidade);
				manager.setTipoUnidade(tipoUnidade);
			}
		}
		
		LOGGER.info("**********************FINALIZANDO METODO alterUnityManagers**********************");
		
		return managers;
	}
	
	*//**
	 * Metodo para verificar a parametrização de perfil do gerente
	 * 
	 * @param csv
	 * @param profile
	 **//*
	// Resolver perfil ??
	private static List<Manager> verifyManagerProfile(final String profile, final List<Manager> managers) {
		List<Manager> gerentesGrupoPerfil = null;
		if (profile != null && managers != null) {
			gerentesGrupoPerfil = new ArrayList<Manager>();
			for (Manager manager : managers) {
				if (!manager.getGrupoPerfil().isEmpty()) {
					if (profile.equals(manager.getGrupoPerfil())) {
						gerentesGrupoPerfil.add(manager);
					}
				}
			}
		}
		return gerentesGrupoPerfil;
	}
	public List<Manager> searchManagerNucleoEmpresa(final List<Hierarquia> listaHierarquia,
			final TableConfiguration tableConfiguration) throws IOException {
		LOGGER.info("**********************INICIANDO METODO searchManagerNucleoEmpresa**********************");
		final HBaseDAO hDAO = new HBaseDAO();
		hDAO.hbaseConfig(tableConfiguration.getPathHbaseSite(), tableConfiguration.getPathKeyTab(),
				tableConfiguration.getUserKeyTab());
		List<Manager> listManagerGG = null;
		List<Manager> listManagerGA = null;
		List<Manager> managerList = new ArrayList<Manager>();
		final CSVReader csv = new CSVReader(tableConfiguration.getPath1());
		final List<Comercial> listComercial = csv.getComercialOperacional();
		
		for (Hierarquia unidadePonteiro : listaHierarquia) {
			
			LOGGER.info("Unidade Ponteiro: " +unidadePonteiro);
			listManagerGG = searchManager(
					Utils.generateKey(FrontUnicoLakeConstantes.SEPADOR_HIFEN, Utils.leftPad(unidadePonteiro.getTipoUnidade()),
							unidadePonteiro.getUnidade()),
					FrontUnicoLakeConstantes.TABLE_NAME_GERENTES_UNIDADES, FrontUnicoLakeConstantes.COLUMN_FAMILY_NAME_GU,
					FrontUnicoLakeConstantes.TABLE_MANAGER, hDAO);
			listManagerGG = verifyManagerProfile(FrontUnicoLakeConstantes.GG, listManagerGG);
			listManagerGG = alterUnityManagers(unidadePonteiro.getUnidade(), unidadePonteiro.getTipoUnidade(),
					listManagerGG);
			
			if(CollectionUtils.isNotEmpty(listManagerGG)) {
				managerList.addAll(listManagerGG);
				LOGGER.info("listManagerGG: " +listManagerGG);
			}
			final Comercial comercial = buscarParametrizacaoNucleoEmpresa(unidadePonteiro.getUnidade(), Utils.leftPad(unidadePonteiro.getTipoUnidade()),
					"015", listComercial);
			
			if(comercial != null) {
				listManagerGA = searchManager(
						Utils.generateKey(FrontUnicoLakeConstantes.SEPADOR_HIFEN, comercial.getOperationUniOrgType(),
								comercial.getOperationUniOrgCode()),
						FrontUnicoLakeConstantes.TABLE_NAME_GERENTES_UNIDADES, FrontUnicoLakeConstantes.COLUMN_FAMILY_NAME_GU,
						FrontUnicoLakeConstantes.TABLE_MANAGER, hDAO);
				
				listManagerGA = verifyManagerProfile(FrontUnicoLakeConstantes.GA, listManagerGA);
				
				listManagerGA = alterUnityManagers(unidadePonteiro.getUnidade(), Utils.leftPad(unidadePonteiro.getTipoUnidade()),
						listManagerGA);
				
				if(CollectionUtils.isNotEmpty(listManagerGA)) {
					managerList.addAll(listManagerGA);
					LOGGER.info("listManagerGA: " +listManagerGA);
				}
				 
			} else {
				LOGGER.info("**********************PARAMETRIZACAO NAO ENCONTRADA**********************");
			}
			
		}
		
		LOGGER.info("**********************FINALIZANDO METODO searchManagerNucleoEmpresa**********************");
		return managerList;
	}
	
	public List<Manager> searchManagerAtendimentoDigital(final List<Hierarquia> listaHierarquia,
			final TableConfiguration tableConfiguration) throws IOException {
		LOGGER.info("**********************INICIANDO METODO searchManagerAtendimentoDigital**********************");
		final HBaseDAO hDAO = new HBaseDAO();
		hDAO.hbaseConfig(tableConfiguration.getPathHbaseSite(), tableConfiguration.getPathKeyTab(),
				tableConfiguration.getUserKeyTab());
		List<Manager> listManagerGG = null;
		List<Manager> listManagerGA = null;
		List<Manager> managerList = new ArrayList<Manager>();
		final CSVReader csv = new CSVReader(tableConfiguration.getPath1());
		final List<Comercial> listComercial = csv.getComercialOperacional();
		for (Hierarquia unidadePonteiro : listaHierarquia) {
			LOGGER.info("Unidade Ponteiro: " +unidadePonteiro);
			listManagerGG = searchManager(
					Utils.generateKey(FrontUnicoLakeConstantes.SEPADOR_HIFEN, Utils.leftPad(unidadePonteiro.getTipoUnidade()), 
							unidadePonteiro.getUnidade()),
					FrontUnicoLakeConstantes.TABLE_NAME_GERENTES_UNIDADES, FrontUnicoLakeConstantes.COLUMN_FAMILY_NAME_GU,
					FrontUnicoLakeConstantes.TABLE_MANAGER, hDAO);
			listManagerGG = verifyManagerProfile(FrontUnicoLakeConstantes.GG, listManagerGG);
			listManagerGG = alterUnityManagers(unidadePonteiro.getUnidade(), unidadePonteiro.getTipoUnidade(),
					listManagerGG);
			
			if(CollectionUtils.isNotEmpty(listManagerGG)) {
				managerList.addAll(listManagerGG);
				LOGGER.info("listManagerGG: " +listManagerGG);
			}
			final Comercial comercial = buscarParametrizacaoAtendimentoDigital(unidadePonteiro.getUnidade(), 
					Utils.leftPad(unidadePonteiro.getTipoUnidade()), listComercial);
			if(comercial != null) {
				listManagerGA = searchManager(
						Utils.generateKey(FrontUnicoLakeConstantes.SEPADOR_HIFEN, Utils.leftPad(comercial.getOperationUniOrgType()),
								comercial.getOperationUniOrgCode()),
						FrontUnicoLakeConstantes.TABLE_NAME_GERENTES_UNIDADES, FrontUnicoLakeConstantes.COLUMN_FAMILY_NAME_GU,
						FrontUnicoLakeConstantes.TABLE_MANAGER, hDAO);
				
				listManagerGA = verifyManagerProfile(FrontUnicoLakeConstantes.GA, listManagerGA);
				
				listManagerGA = alterUnityManagers(unidadePonteiro.getUnidade(), unidadePonteiro.getTipoUnidade(),
						listManagerGA);
				
				if(CollectionUtils.isNotEmpty(listManagerGA)) {
					managerList.addAll(listManagerGA);
					LOGGER.info("listManagerGA: " +listManagerGA);
				}
			}
		}
		
		LOGGER.info("**********************FINALIZANDO METODO searchManagerAtendimentoDigital**********************");
		return managerList;
	}
	
	*//**
	 * Metodo para buscar na Parametrização de Relacionamento entre Unidade
	 * Comercial e Unidade Operacional qual a unidade Operacional associada a Uniorg
	 * 
	 * @param codigoUnidade
	 * @param tableConfiguration
	 * @return the comercial
	 **//*
	public static Comercial buscarParametrizacaoNucleoEmpresa(final String codigoUnidadeComercial, final String codigoTipoComercial, final String codigoTipoOperacional,
			final List<Comercial> listComercial) {
		LOGGER.info("***************** INICIANDO METODO verifyUniOrg ************************");
		
		Comercial comercial = null;
		
		if(listComercial != null) {
			LOGGER.info("size listComercial: " +listComercial.size());
			
			for(Comercial comercialCsv :listComercial) {
				if(comercialCsv.getComercialUniOrgCode().equals(codigoUnidadeComercial) 
						&& comercialCsv.getComercialUniOrgType().equals(codigoTipoComercial)
						&& comercialCsv.getOperationUniOrgType().equals(codigoTipoOperacional)) {
					comercial = comercialCsv;
				}
			}
		}
		
		LOGGER.info("comercial: " +comercial);
		LOGGER.info("***************** FINALIZANDO METODO verifyUniOrg ************************");
		return comercial;
	}
	
	public static Comercial buscarParametrizacaoAtendimentoDigital(final String codigoUnidadeComercial, final String codigoTipoComercial, final List<Comercial> listComercial) {
		LOGGER.info("***************** INICIANDO METODO verifyUniOrg ************************");
		
		Comercial comercial = null;
		
		if(listComercial != null) {
			for(Comercial comercialCsv :listComercial) {
				if(StringUtils.isNotEmpty(comercialCsv.getComercialUniOrgCode()) && StringUtils.isNotEmpty(comercialCsv.getComercialUniOrgType())
						&& StringUtils.isNotEmpty(comercialCsv.getOperationUniOrgType())&& comercialCsv.getComercialUniOrgCode().equals(codigoUnidadeComercial) 
						&& comercialCsv.getComercialUniOrgType().equals(codigoTipoComercial)) {
					comercial = comercialCsv;
				}
			}
		}
		
		LOGGER.info("comercial: " +comercial);
		LOGGER.info("***************** FINALIZANDO METODO verifyUniOrg ************************");
		return comercial;
	}
	
	private static T0004 searchLowerUnit(final String tipoUnidadeSuperior, final String codigoUnidadeSuperior,
			final String tipoRelacionamento, final String tipoUnidadeInferior, final DataFrame t0004DF) {
		
		LOGGER.info("***************** INICIANDO METODO searchLowerUnit ************************");
		
		StringBuilder whereDataFrame = new StringBuilder()
				.append("tipoUnidadeSuperior = ").append(tipoUnidadeSuperior)
				.append(" AND codigoUnidadeSuperior = ").append(codigoUnidadeSuperior)
				.append(" AND tipoRelacionamento = ").append(tipoRelacionamento)
				.append(" AND tipoUnidadeInferior = ").append(tipoUnidadeInferior);
		
		LOGGER.info("whereDataFrame: " +whereDataFrame);
		
		DataFrame result = t0004DF.where(whereDataFrame.toString());
		JavaRDD<T0004> t0004RDD = result.toJavaRDD().map(new BuscaT0004());
		final List<T0004> listT0004 = t0004RDD.collect();
		
		T0004 t0004 = null;
		
		if(!listT0004.isEmpty())
			t0004 = listT0004.get(0);
		
		LOGGER.info("t0004: " +t0004);
		LOGGER.info("***************** FINALIZANDO METODO searchLowerUnit ************************");
		return t0004;
	}
	private static T0004 searchUpperUnit(final String tipoUnidadeInferior, final String codigoUnidadeInferior,
			final String tipoRelacionamento, final DataFrame t0004DF) {
		
		LOGGER.info("***************** INICIANDO METODO searchUpperUnit ************************");
		
		StringBuilder whereDataFrame = new StringBuilder()
				.append("tipoUnidadeInferior = ").append(tipoUnidadeInferior)
				.append(" AND codigoUnidadeInferior = ").append(codigoUnidadeInferior)
				.append(" AND tipoRelacionamento = ").append(tipoRelacionamento);
		
		LOGGER.info("whereDataFrame: " +whereDataFrame);
		
		DataFrame result = t0004DF.where(whereDataFrame.toString());
		JavaRDD<T0004> t0004RDD = result.toJavaRDD().map(new BuscaT0004());
		final List<T0004> listT0004 = t0004RDD.collect();
		
		T0004 t0004 = null;
		
		if(!listT0004.isEmpty())
			t0004 = listT0004.get(0);
		
		LOGGER.info("t0004: " +t0004);
		LOGGER.info("***************** FINALIZANDO METODO searchUpperUnit ************************");
		
		return t0004;
	}
	
	public static class BuscaT0004 implements Function<Row, T0004> {
		private static final long serialVersionUID = 1L;
		public T0004 call(Row row) throws Exception {
			T0004 t0004 = new T0004();
			t0004.setTipoUnidadeInferior(row.getString(0));
			t0004.setCodigoUnidadeInferior(row.getString(1));
			t0004.setTipoUnidadeSuperior(row.getString(2));
			t0004.setCodigoUnidadeSuperior(row.getString(3));
			t0004.setTipoRelacionamento(row.getString(4));
			return t0004;
		}
	}
	
}
*/