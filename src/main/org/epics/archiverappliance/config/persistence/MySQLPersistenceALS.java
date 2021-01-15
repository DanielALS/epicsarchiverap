package org.epics.archiverappliance.config.persistence;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ConfigPersistence;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.UserSpecifiedSamplingParams;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Custom persistence layer to use MySQL DB whose table  PVTypeINfo has the columns taken
 * from the keys.
 */
public class MySQLPersistenceALS implements ConfigPersistence {
	private static Logger configlogger = Logger.getLogger("config." + MySQLPersistenceALS.class.getName());
	private static Logger logger = Logger.getLogger(MySQLPersistenceALS.class.getName());
	private DataSource theDataSource;

	public MySQLPersistenceALS() throws ConfigException {
		try {
			configlogger.info("Looking up datasource called jdbc/archappl in the java:/comp/env namespace using JDNI");
			Context initContext = new InitialContext();
			Context envContext  = (Context) initContext.lookup("java:/comp/env");
			theDataSource = (DataSource)envContext.lookup("jdbc/archappl");
			configlogger.info("Found datasource called jdbc/archappl in the java:/comp/env namespace using JDNI");
		} catch(Exception ex) {
			throw new ConfigException("Exception initializing MySQLPersistence ", ex);
		}
	}

    private static final String keys = "hostName,paused,creationTime,lowerAlarmLimit,precision,lowerCtrlLimit,units,computedBytesPerEvent,computedEventRate,usePVAccess,computedStorageRate,modificationTime,upperDisplayLimit,upperWarningLimit,DBRType,sts,mts,lts,upperAlarmLimit,userSpecifiedEventRate,useDBEProperties,hasReducedDataSet,lowerWarningLimit,chunkKey,applianceIdentity,scalar,upperCtrlLimit,lowerDisplayLimit,samplingPeriod,elementCount,samplingMethod,archiveFields,extraFields";

    /* stays the same */
	@Override
	public List<String> getTypeInfoKeys() throws IOException {
		return getKeys("SELECT pvName AS pvName FROM PVTypeInfo ORDER BY pvName;", "getTypeInfoKeys");
	}

	@Override
	public PVTypeInfo getTypeInfo(String pvName) throws IOException {
        String query = "SELECT " + "pvName," + keys + ",lastModified " + "FROM PVTypeInfo WHERE pvName = ?;";
		return getValues(query, pvName, new PVTypeInfo(), "getTypeInfo");
	}

	@Override
	public void putTypeInfo(String pvName, PVTypeInfo typeInfo) throws IOException {
        String query = "INSERT INTO PVTypeInfo (pvName," + keys + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        query += " ON DUPLICATE KEY UPDATE (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
		putValueForKey(query , pvName, typeInfo, PVTypeInfo.class, "putTypeInfo");
	}

	@Override
	public void deleteTypeInfo(String pvName) throws IOException {
		removeKey("DELETE FROM PVTypeInfo WHERE pvName = ?;", pvName, "deleteTypeInfo");
	}

	@Override
	public List<String> getArchivePVRequestsKeys() throws IOException {
		return getKeys("SELECT pvName AS pvName FROM ArchivePVRequests ORDER BY pvName;", "getArchivePVRequestsKeys");
	}

	@Override
	public UserSpecifiedSamplingParams getArchivePVRequest(String pvName) throws IOException {
		return getValueForKey("SELECT userParams AS userParams FROM ArchivePVRequests WHERE pvName = ?;", pvName, new UserSpecifiedSamplingParams(), UserSpecifiedSamplingParams.class, "getArchivePVRequest");
	}

	@Override
	public void putArchivePVRequest(String pvName, UserSpecifiedSamplingParams userParams) throws IOException {
		putValueForKey("INSERT INTO ArchivePVRequests (pvName, userParams) VALUES (?, ?) ON DUPLICATE KEY UPDATE userParams = ?;", pvName, userParams, UserSpecifiedSamplingParams.class, "putArchivePVRequest");
	}

	@Override
	public void removeArchivePVRequest(String pvName) throws IOException {
		removeKey("DELETE FROM ArchivePVRequests WHERE pvName = ?;", pvName, "removeArchivePVRequest");
	}

	@Override
	public List<String> getExternalDataServersKeys() throws IOException {
		return getKeys("SELECT serverid AS serverid FROM ExternalDataServers ORDER BY serverid;", "getExternalDataServersKeys");
	}

	@Override
	public String getExternalDataServer(String serverId) throws IOException {
		return getStringValueForKey("SELECT serverinfo AS serverinfo FROM ExternalDataServers WHERE serverid = ?;", serverId, "getExternalDataServer");
	}

	@Override
	public void putExternalDataServer(String serverId, String serverInfo) throws IOException {
		putStringValueForKey("INSERT INTO ExternalDataServers (serverid, serverinfo) VALUES (?, ?) ON DUPLICATE KEY UPDATE serverinfo = ?;", serverId, serverInfo, "putExternalDataServer");
	}

	@Override
	public void removeExternalDataServer(String serverId, String serverInfo) throws IOException {
		removeKey("DELETE FROM ExternalDataServers WHERE serverid = ?;", serverId, "removeExternalDataServer");
	}

	@Override
	public List<String> getAliasNamesToRealNamesKeys() throws IOException {
		return getKeys("SELECT pvName AS pvName FROM PVAliases ORDER BY pvName;", "getAliasNamesToRealNamesKeys");
	}

	@Override
	public String getAliasNamesToRealName(String pvName) throws IOException {
		return getStringValueForKey("SELECT realName AS realName FROM PVAliases WHERE pvName = ?;", pvName, "getAliasNamesToRealName");
	}

	@Override
	public void putAliasNamesToRealName(String pvName, String realName) throws IOException {
		putStringValueForKey("INSERT INTO PVAliases (pvName, realName) VALUES (?, ?) ON DUPLICATE KEY UPDATE realName = ?;", pvName, realName, "putAliasNamesToRealName");
	}

	@Override
	public void removeAliasName(String pvName, String realName) throws IOException {
		removeKey("DELETE FROM PVAliases WHERE pvName = ?;", pvName, "removeAliasName");
	}


	private List<String> getKeys(String sql, String msg) throws IOException {
		LinkedList<String> ret = new LinkedList<String>();
		try(Connection conn = theDataSource.getConnection()) {
			try(PreparedStatement stmt = conn.prepareStatement(sql)) {
				try(ResultSet rs = stmt.executeQuery()) {
					while(rs.next()) {
						String pvName = rs.getString(1);
						ret.add(pvName);
					}
				}
			}
		} catch(SQLException ex) {
			throw new IOException(ex);
		}
		logger.debug(msg + " returns " + ret.size() + " keys");
		return ret;
	}


    private Timestamp convertString2Date(String date_string) throws ParseException {
       SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-dd-mmTHH:MM:SS.sssZ");
       Date parsedDate = dateFormat.parse(date_string);
       Timestamp timestamp = new Timestamp(parsedDate.getTime());
       return timestamp;
    }

	private PVTypeInfo getValues(String sql, String pvName, PVTypeInfo obj, String msg) throws IOException {
        if (pvName == null || pvName.equals("")) return null;

		try(Connection conn = theDataSource.getConnection()) {
			try(PreparedStatement stmt = conn.prepareStatement(sql)) {
				stmt.setString(1, pvName);
				try(ResultSet rs = stmt.executeQuery()) {
					while(rs.next()) {
                        obj.setHostName(rs.getString("hostName"));
                        obj.setPaused(Boolean.parseBoolean(rs.getString("paused")));
                        obj.setCreationTime(convertString2Date(rs.getString("creationTime")));
                        obj.setLowerAlarmLimit(rs.getDouble("alarmLimit"));
                        obj.setPrecision(rs.getDouble("precision"));
                        obj.setLowerCtrlLimit(rs.getDouble("lowerCtrlLimit"));
                        obj.setUnits(rs.getString("units"));
                        obj.setComputedBytesPerEvent(rs.getInt("computedBytesPerEvent"));
                        obj.setComputedEventRate(rs.getFloat("computedEventRate"));
                        obj.setUsePVAccess(Boolean.parseBoolean(rs.getString("usePVAccess")));
                        obj.setComputedStorageRate(rs.getFloat("computedStorageRate"));
                        obj.setModificationTime(convertString2Date(rs.getString("modificationTime")));
                        obj.setUpperDisplayLimit(rs.getDouble("upperDisplayLimit"));
                        obj.setUpperWarningLimit(rs.getDouble("upperWarningLimit"));

                        String dbrtype = rs.getString("DBRType");
                        obj.setDBRType(ArchDBRTypes.valueOf(dbrtype));

                        String[] ds = new String[3];
                        ds[0] = (rs.getString("sts"));
                        ds[1] = (rs.getString("mts"));
                        ds[2] = (rs.getString("lts"));
                        obj.setDataStores(ds);

                        obj.setUpperAlarmLimit(rs.getDouble("upperAlarmLimit"));
                        obj.setUserSpecifiedEventRate(rs.getFloat("userspecifiedeventrate"));
                        obj.setUseDBEProperties(Boolean.parseBoolean(rs.getString("useDBEProperties")));
                        obj.setHasReducedDataSet(Boolean.parseBoolean(rs.getString("hasReducedDataSet")));
                        obj.setLowerWarningLimit(rs.getDouble("lowerWarningLimit"));
                        obj.setChunkKey(rs.getString("chunkKey"));
                        obj.setApplianceIdentity(rs.getString("applianceIdentity"));
                        obj.setScalar(Boolean.parseBoolean(rs.getString("scalar")));
                        obj.setPvName(rs.getString("pvName"));
                        obj.setUpperCtrlLimit(rs.getDouble("upperCtrlLimit"));
                        obj.setLowerDisplayLimit(rs.getDouble("lowerDisplayLimit"));
                        obj.setSamplingPeriod(rs.getFloat("samplingPeriod"));
                        obj.setElementCount(rs.getInt("elementCount"));

                        String sm = rs.getString("samplingMethod");
                        obj.setSamplingMethod(SamplingMethod.valueOf(sm));

                        /* becomes a hash map
                         * This looks like an extensible field to hold any number of
                         * key value pairs. That makes it hard to implement in this
                         * SQL table idea. However, we do not use this filed
                         * in any channels atm. The ArchiveFields can be null.
                        */
                        obj.setArchiveFields(new String[0]);

                        /*
                         * Extra fields are static names held in columns.
                        */
                        HashMap<String, String> extraFields = new HashMap<String, String>();
                        extraFields.put("RTYP", rs.getString("rtype"));
                        extraFields.put("MDEL", String.valueOf(rs.getFloat("mdel")));
                        extraFields.put("ADEL", String.valueOf(rs.getFloat("adel")));
                        extraFields.put("SCAN", rs.getString("scan"));
                        obj.setExtraFields(extraFields);

                        return obj;
					}
				}
			}
		}
        catch(Exception ex) {
			throw new IOException(ex);
		}

		return null;
	}

	private String getStringValueForKey(String sql, String key, String msg) throws IOException {
		if(key == null || key.equals("")) return null;

		try(Connection conn = theDataSource.getConnection()) {
			try(PreparedStatement stmt = conn.prepareStatement(sql)) {
				stmt.setString(1, key);
				try(ResultSet rs = stmt.executeQuery()) {
					while(rs.next()) {
						return rs.getString(1);
					}
				}
			}
		} catch(Exception ex) {
			throw new IOException(ex);
		}

		return null;
	}



	private <T> void putValueForKey(String sql, String key, T obj, Class<T> clazz, String msg) throws IOException {
		if(key == null || key.equals("")) throw new IOException("key cannot be null when persisting " + msg);
		if(obj == null || obj.equals("")) throw new IOException("value cannot be null when persisting " + msg);

		try(Connection conn = theDataSource.getConnection()) {
			JSONEncoder<T> encoder = JSONEncoder.getEncoder(clazz);
			JSONObject jsonObj = encoder.encode(obj);
			String jsonStr = jsonObj.toJSONString();

			try(PreparedStatement stmt = conn.prepareStatement(sql)) {
				stmt.setString(1, key);
				stmt.setString(2, jsonStr);
				stmt.setString(3, jsonStr);
				int rowsChanged = stmt.executeUpdate();
				if(rowsChanged != 1) {
					logger.warn(rowsChanged + " rows changed when updating key  " + key + " in " + msg);
				} else {
					logger.debug("Successfully updated value for key " + key + " in " + msg);
				}
			}
		} catch(Exception ex) {
			throw new IOException(ex);
		}
	}


	private void putStringValueForKey(String sql, String key, String value, String msg) throws IOException {
		if(key == null || key.equals("")) throw new IOException("key cannot be null when persisting " + msg);
		if(value == null || value.equals("")) throw new IOException("value cannot be null when persisting " + msg);

		try(Connection conn = theDataSource.getConnection()) {
			try(PreparedStatement stmt = conn.prepareStatement(sql)) {
				stmt.setString(1, key);
				stmt.setString(2, value);
				stmt.setString(3, value);
				int rowsChanged = stmt.executeUpdate();
				if(rowsChanged != 1) {
					logger.warn(rowsChanged + " rows changed when updating key  " + key + " in " + msg);
				} else {
					logger.debug("Successfully updated value for key " + key + " in " + msg);
				}
			}
		} catch(Exception ex) {
			throw new IOException(ex);
		}
	}


	private void removeKey(String sql, String key, String msg) throws IOException {
		try(Connection conn = theDataSource.getConnection()) {
			try(PreparedStatement stmt = conn.prepareStatement(sql)) {
				stmt.setString(1, key);
				int rowsChanged = stmt.executeUpdate();
				if(rowsChanged != 1) {
					logger.warn(rowsChanged + " rows changed when removing key  " + key + " in " + msg);
				} else {
					logger.debug("Successfully removed key " + key + " in " + msg);
				}
			}
		} catch(SQLException ex) {
			throw new IOException(ex);
		}
	}

	private <T> T getValueForKey(String sql, String key, T obj, Class<T> clazz, String msg) throws IOException {
		if(key == null || key.equals("")) return null;

		try(Connection conn = theDataSource.getConnection()) {
			try(PreparedStatement stmt = conn.prepareStatement(sql)) {
				stmt.setString(1, key);
				try(ResultSet rs = stmt.executeQuery()) {
					while(rs.next()) {
						String jsonStr = rs.getString(1);
						JSONObject jsonObj = (JSONObject) JSONValue.parse(jsonStr);
						JSONDecoder<T> decoder = JSONDecoder.getDecoder(clazz);
						decoder.decode(jsonObj, obj);
						return obj;
					}
				}
			}
		} catch(Exception ex) {
			throw new IOException(ex);
		}

		return null;
	}
}
