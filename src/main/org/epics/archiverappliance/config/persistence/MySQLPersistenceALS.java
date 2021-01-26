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
import java.util.Arrays;
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
        String query = "INSERT INTO PVTypeInfo (pvName," + keys + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        query += " ON DUPLICATE KEY UPDATE (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
		updatePVTypeInfo(query, pvName, typeInfo, "putTypeInfo");
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
        String query = "SELECT samplingMethod, skipAliasCheck, skipCapacityPlanning, samplingPeriod, controllingPV, policyName, usePVAcess, alias, archiveFields FROM ArchivePVRequests WHERE pvName = ?;";
		return get_archival_params(query, pvName, new UserSpecifiedSamplingParams(), "getArchivePVRequest");
	}

	@Override
	public void putArchivePVRequest(String pvName, UserSpecifiedSamplingParams userParams) throws IOException {
        String query = "INSERT INTO ArchivePVRequests (pvName, samplingMethod, samplingPeriod, controllingPV, policyName, usePVAcess, last_modifed) VALUES (?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE (?, ?, ?, ?, ?, ?, ?);";
		putArchiveRequestParams(query, pvName, userParams, "putArchivePVRequest");
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


	private void updatePVTypeInfo(String sql, String pvName, PVTypeInfo obj, String msg) throws IOException {
		if(pvName == null || pvName.equals("")) throw new IOException("pvName cannot be null when updating: " + msg);
		if(obj == null || obj.equals("")) throw new IOException("value cannot be null when updating:" + msg);

        String[] cols = keys.split("\\s*,\\s*");

		try(Connection conn = theDataSource.getConnection()) {
			try(PreparedStatement stmt = conn.prepareStatement(sql)) {
				stmt.setString(1, pvName);
				stmt.setString(2, cols[0]);
				stmt.setTimestamp(3, convertString2Date(cols[1]));
				stmt.setDouble(4, Double.parseDouble(cols[2]));
				stmt.setDouble(5, Double.parseDouble(cols[3]));
				stmt.setDouble(6, Double.parseDouble(cols[4]));
				stmt.setInt(7, Integer.valueOf(cols[5]));
				stmt.setDouble(8, Double.parseDouble(cols[6]));
				stmt.setString(9, cols[7]);
				stmt.setDouble(10, Double.parseDouble(cols[8]));
				stmt.setTimestamp(11, convertString2Date(cols[9]));
				stmt.setDouble(12, Double.parseDouble(cols[10]));
				stmt.setDouble(13, Double.parseDouble(cols[11]));
				stmt.setString(14, cols[12]);
				stmt.setString(15, cols[13]);
				stmt.setString(16, cols[14]);
				stmt.setString(17, cols[15]);
				stmt.setDouble(18, Double.parseDouble(cols[16]));
				stmt.setDouble(19, Double.parseDouble(cols[17]));
				stmt.setString(20, cols[18]);
				stmt.setString(21, cols[19]);
				stmt.setDouble(22, Double.parseDouble(cols[20]));
				stmt.setString(23, cols[21]);
				stmt.setString(24, cols[22]);
				stmt.setDouble(25, Double.parseDouble(cols[23]));
				stmt.setDouble(26, Double.parseDouble(cols[24]));
				stmt.setDouble(27, Double.parseDouble(cols[25]));
				stmt.setInt(28, Integer.valueOf(cols[26]));
				stmt.setString(29, cols[27]);
				stmt.setString(30, cols[28]);
				stmt.setFloat(30, Float.parseFloat(cols[28]));
				stmt.setFloat(31, Float.parseFloat(cols[29]));
				stmt.setFloat(32, Float.parseFloat(cols[30]));
				stmt.setString(33, cols[31]);
				stmt.setTimestamp(34, convertString2Date(cols[33]));

                int rowsChanged = stmt.executeUpdate();

				if(rowsChanged != 1) {
					logger.warn(rowsChanged + " rows changed when updating PV " + pvName + " in " + msg);
				} else {
					logger.debug("Successfully updated value for PV " + pvName + " in " + msg);
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

        /* 1 userSpecifiedSamplingMethod,
         * 2 skipAliasCheck ENUM('true', 'false'),
         * 3 skipCapacityPlanning ENUM('true', 'false'),
         * 4 userSpecifiedSamplingPeriod,
         * 5 controllingPV,
         * 6 policyName,
         * 7 usePVAccess ENUM("true", "false"),
         * 8 alias,
         * 9 archiveFields,
         * 10 `last_modified` TIMESTAMP  # not sure if we need to select for this ?*/
	private UserSpecifiedSamplingParams get_archival_params(String sql, String pvName, UserSpecifiedSamplingParams obj, String msg) throws IOException {
		if(pvName == null || pvName.equals("")) return null;

		try(Connection conn = theDataSource.getConnection()) {
			try(PreparedStatement stmt = conn.prepareStatement(sql)) {
				stmt.setString(1, pvName);
				try(ResultSet rs = stmt.executeQuery()) {
					while(rs.next()) {
						obj.setUserSpecifedsamplingMethod(SamplingMethod.valueOf(rs.getString(1)));
						obj.setSkipAliasCheck(rs.getBoolean(2));
						obj.setSkipCapacityPlanning(rs.getBoolean(3));
						obj.setUserSpecifedSamplingPeriod(rs.getFloat(4));
						obj.setControllingPV(rs.getString(5));
						obj.setPolicyName(rs.getString(6));
						obj.setUsePVAccess(rs.getBoolean(7));
                        String alias_string = rs.getString(8);
                        String[] aliases = alias_string.split(";");
						obj.setAliases(aliases);
                        // Strings delimited by ;
                        String fieldstring = rs.getString(9);
                        String[] fields = fieldstring.split("\\s*,\\s*");
						obj.setArchiveFields(fields);
						return obj;
					}
				}
			}
		} catch(Exception ex) {
			throw new IOException(ex);
		}

		return null;
	}

        /* 1 pvName
         * 2 userSpecifiedSamplingMethod,
         * 3 skipAliasCheck ENUM('true', 'false'),
         * 4 skipCapacityPlanning ENUM('true', 'false'),
         * 5 userSpecifiedSamplingPeriod,
         * 6 controllingPV,
         * 7 policyName,
         * 8 usePVAccess ENUM("true", "false"),
         * 9 alias,
         * 10 archiveFields,
         * 11 `last_modified` TIMESTAMP  # not sure if we need to select for this ?*/

     private void putArchiveRequestParams(String query, String pvName, UserSpecifiedSamplingParams userParams, String msg) throws IOException {
		if(pvName == null || pvName.equals("")) throw new IOException("pvName cannot be null when updating:" + msg);
		try(Connection conn = theDataSource.getConnection()) {
			try(PreparedStatement stmt = conn.prepareStatement(query)) {
				stmt.setString(1, pvName);
                SamplingMethod method = userParams.getUserSpecifedsamplingMethod();
				stmt.setString(2, method.toString());
				stmt.setString(3, String.valueOf(userParams.isSkipAliasCheck()));
				stmt.setString(4, String.valueOf(userParams.isSkipCapacityPlanning()));
				stmt.setDouble(5, userParams.getUserSpecifedSamplingPeriod());
				stmt.setString(6, userParams.getControllingPV());
				stmt.setString(7, userParams.getPolicyName());
				stmt.setString(8, String.valueOf(userParams.isUsePVAccess()));
				stmt.setString(9, String.valueOf(userParams.isUsePVAccess()));
                // String using ; delimiter to store list.
                String[] fields = userParams.getArchiveFields();
                String fields_string = String.join(";", fields);
				stmt.setString(10, fields_string);
				int rowsChanged = stmt.executeUpdate();
				if(rowsChanged != 1) {
					logger.warn(rowsChanged + " rows changed when updating key  " + pvName + " in " + msg);
				} else {
					logger.debug("Successfully updated value for key " + pvName + " in " + msg);
				}
			}
		} catch(Exception ex) {
			throw new IOException(ex);
		}
	}
}
