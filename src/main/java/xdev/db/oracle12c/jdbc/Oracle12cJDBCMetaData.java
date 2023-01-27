/*
 * SqlEngine Database Adapter Oracle 12c - XAPI SqlEngine Database Adapter for Oracle 12c
 * Copyright © 2003 XDEV Software (https://xdev.software)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package xdev.db.oracle12c.jdbc;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.xdev.jadoth.sqlengine.exceptions.SQLEngineInvalidIdentifier;
import com.xdev.jadoth.sqlengine.internal.SqlIdentifier;

import xdev.db.ColumnMetaData;
import xdev.db.DBConnection;
import xdev.db.DBException;
import xdev.db.DataType;
import xdev.db.Index;
import xdev.db.Index.IndexType;
import xdev.db.Result;
import xdev.db.StoredProcedure;
import xdev.db.StoredProcedure.Param;
import xdev.db.StoredProcedure.ParamType;
import xdev.db.StoredProcedure.ReturnTypeFlavor;
import xdev.db.jdbc.JDBCConnection;
import xdev.db.jdbc.JDBCDataSource;
import xdev.db.jdbc.JDBCMetaData;
import xdev.db.jdbc.JDBCResult;
import xdev.util.CollectionUtils;
import xdev.util.ProgressMonitor;
import xdev.util.StringUtils;
import xdev.vt.VirtualTable;
import xdev.vt.VirtualTable.VirtualTableRow;


public class Oracle12cJDBCMetaData extends JDBCMetaData
{
	private static final long	serialVersionUID		= -5500834377631910671L;
	
	private static final int	MAX_IDENTIFIER_LENGTH	= 30;
	
	protected static final char	DELIM					= '"';
	
	
	public Oracle12cJDBCMetaData(Oracle12cJDBCDataSource dataSource) throws DBException
	{
		super(dataSource);
	}
	
	
	@Override
	public boolean isCaseSensitive() throws DBException
	{
		return false;
	}
	
	
	@Override
	protected String getCatalog(JDBCDataSource dataSource)
	{
		return null;
	}
	
	
	@Override
	protected String getSchema(JDBCDataSource dataSource)
	{
		String schema = super.getSchema(dataSource);
		if(schema == null || schema.length() == 0)
		{
			schema = dataSource.getUserName().toUpperCase();
		}
		
		return schema;
	}
	
	
	@Override
	public TableInfo[] getTableInfos(ProgressMonitor monitor, EnumSet<TableType> types)
			throws DBException
	{
		monitor.beginTask("",ProgressMonitor.UNKNOWN);
		
		List<TableInfo> list = new ArrayList<>();
		
		JDBCConnection jdbcConnection = (JDBCConnection)this.dataSource.openConnection();
		
		try
		{
			String schema = getSchema(this.dataSource);
			
			if(types.contains(TableType.TABLE))
			{
				Result result = jdbcConnection
						.query("SELECT table_name, num_rows FROM user_tables");
				while(result.next() && !monitor.isCanceled())
				{
					String name = result.getString("table_name");
					TableInfo tableInfo = new TableInfo(TableType.TABLE,schema,name);
					tableInfo.putClientProperty("num_rows",result.getInt("num_rows"));
					list.add(tableInfo);
				}
				result.close();
			}
			
			if(types.contains(TableType.VIEW))
			{
				Result result = jdbcConnection.query("SELECT view_name FROM user_views");
				while(result.next() && !monitor.isCanceled())
				{
					String name = result.getString("view_name");
					list.add(new TableInfo(TableType.VIEW,schema,name));
				}
				result.close();
			}
		}
		finally
		{
			jdbcConnection.close();
		}
		
		monitor.done();
		
		TableInfo[] tables = list.toArray(new TableInfo[list.size()]);
		Arrays.sort(tables);
		return tables;
	}
	
	
	@Override
	public TableMetaData[] getTableMetaData(ProgressMonitor monitor, int flags, TableInfo... tables)
			throws DBException
	{
		if(tables == null || tables.length == 0)
		{
			return new TableMetaData[0];
		}
		
		List<TableMetaData> list = new ArrayList<>(tables.length);
		
		JDBCConnection jdbcConnection = (JDBCConnection)this.dataSource.openConnection();
		try
		{
			try
			{
				monitor.beginTask("",ProgressMonitor.UNKNOWN);
				
				DatabaseMetaData meta = jdbcConnection.getConnection().getMetaData();
				ResultSet resultSet = meta.getColumns(null,getSchema(this.dataSource),null,null);
				Result result = new JDBCResult(resultSet);
				VirtualTable vtColumns = new VirtualTable(result,true);
				result.close();
				
				Map<String, List<VirtualTableRow>> columnMap = toMap(vtColumns,"TABLE_NAME");
				Map<String, List<VirtualTableRow>> primaryKeyMap = null;
				Map<String, List<VirtualTableRow>> indexMap = null;
				
				if((flags & INDICES) != 0 && !monitor.isCanceled())
				{
					List params = new ArrayList();
					params.add(getSchema(this.dataSource));
					
					StringBuilder sbPrimaryKeys = new StringBuilder(
							"SELECT c.table_name, c.column_name, ");
					sbPrimaryKeys.append("c.position AS key_seq, c.constraint_name AS pk_name ");
					sbPrimaryKeys.append("FROM all_cons_columns c, all_constraints k ");
					sbPrimaryKeys.append("WHERE k.constraint_type = 'P' ");
					sbPrimaryKeys.append("AND k.owner=? ");
					sbPrimaryKeys.append("AND k.constraint_name = c.constraint_name ");
					sbPrimaryKeys.append("AND k.table_name = c.table_name ");
					sbPrimaryKeys.append("AND k.owner = c.owner ORDER BY TABLE_NAME, key_seq");
					
					Result rs = jdbcConnection.query(sbPrimaryKeys.toString(),params.toArray());
					VirtualTable vtPrimaryKeys = new VirtualTable(rs,true);
					rs.close();
					primaryKeyMap = toMap(vtPrimaryKeys,"TABLE_NAME");
					
					StringBuilder sbIndexes = new StringBuilder(
							"SELECT c.table_name, c.column_name, ");
					sbIndexes.append("c.column_position AS key_seq, c.index_name, k.uniqueness ");
					sbIndexes.append("FROM all_ind_columns c, all_indexes k ");
					sbIndexes.append("WHERE k.owner=? ");
					sbIndexes.append("AND k.index_name = c.index_name ");
					sbIndexes.append("AND k.table_name = c.table_name ");
					sbIndexes.append("AND k.owner = c.index_owner ORDER BY TABLE_NAME, key_seq");
					
					rs = jdbcConnection.query(sbIndexes.toString(),params.toArray());
					VirtualTable vtIndexes = new VirtualTable(rs,true);
					rs.close();
					indexMap = toMap(vtIndexes,"TABLE_NAME");
				}
				
				Map<Object, Object> defaultValueMap = new HashMap<>();
				
				if(!monitor.isCanceled())
				{
					Set<Object> defaultValueSet = new HashSet<>();
					int rc = vtColumns.getRowCount();
					int columnDefaultIndex = vtColumns.getColumnIndex("COLUMN_DEF");
					for(int i = 0; i < rc; i++)
					{
						defaultValueSet.add(vtColumns.getValueAt(i,columnDefaultIndex));
					}
					
					if(defaultValueSet.size() > 0)
					{
						Object[] defaultValues = defaultValueSet.toArray();
						
						try
						{
							StringBuilder sbDefaultValues = new StringBuilder("SELECT ");
							sbDefaultValues.append(StringUtils.concat(",",defaultValues));
							sbDefaultValues.append(" FROM DUAL");
							
							result = jdbcConnection.query(sbDefaultValues.toString());
							try
							{
								result.next();
								for(int i = 0, cc = result.getColumnCount(); i < cc; i++)
								{
									defaultValueMap.put(defaultValues[i],result.getObject(i));
								}
							}
							finally
							{
								result.close();
							}
						}
						catch(DBException e)
						{
							// one or more default values causes errors, e.g.
							// generated columns, query one by one
							
							List<Object> numericDefaultValues = new ArrayList<>();
							List<Object> nonNumericDefaultValues = new ArrayList<>();
							for(Object defaultValue : defaultValues)
							{
								if(defaultValue == null
										|| "null".equalsIgnoreCase(defaultValue.toString()))
								{
									defaultValueMap.put(defaultValue,null);
								}
								else
								{
									try
									{
										Double.parseDouble(String.valueOf(defaultValue));
										numericDefaultValues.add(defaultValue);
									}
									catch(NumberFormatException e1)
									{
										nonNumericDefaultValues.add(defaultValue);
									}
								}
							}
							
							if(numericDefaultValues.size() > 0)
							{
								StringBuilder sbDefaultValues = new StringBuilder("SELECT ");
								sbDefaultValues
										.append(StringUtils.concat(",",numericDefaultValues));
								sbDefaultValues.append(" FROM DUAL");
								
								result = jdbcConnection.query(sbDefaultValues.toString());
								try
								{
									result.next();
									for(int i = 0, cc = result.getColumnCount(); i < cc; i++)
									{
										defaultValueMap.put(numericDefaultValues.get(i),
												result.getObject(i));
									}
								}
								finally
								{
									result.close();
								}
							}
							
							if(nonNumericDefaultValues.size() > 0)
							{
								for(Object value : nonNumericDefaultValues)
								{
									if(monitor.isCanceled())
									{
										break;
									}
									
									try
									{
										result = jdbcConnection.query("SELECT " + value
												+ " FROM DUAL");
										try
										{
											result.next();
											defaultValueMap.put(value,result.getObject(0));
										}
										finally
										{
											result.close();
										}
									}
									catch(DBException dbe)
									{
										// swallow
									}
								}
							}
						}
					}
				}
				
				monitor.beginTask("",tables.length);
				
				int done = 0;
				for(TableInfo table : tables)
				{
					if(monitor.isCanceled())
					{
						break;
					}
					
					monitor.setTaskName(table.getName());
					try
					{
						list.add(getTableMetaData(table,flags,columnMap,primaryKeyMap,indexMap,
								defaultValueMap));
					}
					catch(Exception e)
					{
						e.printStackTrace();
					}
					monitor.worked(++done);
				}
			}
			finally
			{
				jdbcConnection.close();
			}
		}
		catch(SQLException e)
		{
			throw new DBException(this.dataSource,e);
		}
		
		monitor.done();
		
		return list.toArray(new TableMetaData[list.size()]);
	}
	
	
	private TableMetaData getTableMetaData(TableInfo table, int flags,
			Map<String, List<VirtualTableRow>> columnMap,
			Map<String, List<VirtualTableRow>> primaryKeyMap,
			Map<String, List<VirtualTableRow>> indexMap, Map<Object, Object> defaultValueMap)
			throws Exception
	{
		String tableName = table.getName();
		List<VirtualTableRow> columnRows = columnMap.get(tableName);
		int columnCount = columnRows.size();
		ColumnMetaData[] columns = new ColumnMetaData[columnCount];
		for(int i = 0; i < columnCount; i++)
		{
			VirtualTableRow dataRow = columnRows.get(i);
			String columnName = (String)dataRow.get("COLUMN_NAME");
			String caption = null;
			int sqlType = ((Number)dataRow.get("DATA_TYPE")).intValue();
			DataType type = DataType.get(sqlType);
			int length = ((Number)dataRow.get("COLUMN_SIZE")).intValue();
			Object scaleObj = dataRow.get("DECIMAL_DIGITS");
			int scale = scaleObj instanceof Number ? ((Number)scaleObj).intValue() : 0;
			if(scale < 0)
			{
				scale = 0;
			}
			
			if(type == DataType.NUMERIC)
			{
				if(scale <= 0)
				{
					switch(length)
					{
						case 3:
							type = DataType.TINYINT;
						break;
						case 5:
							type = DataType.SMALLINT;
						break;
						case 38:
							type = DataType.BIGINT;
						break;
						default:
							type = DataType.INTEGER;
					}
				}
				else
				{
					length = length - scale;
				}
			}
			
			Object defaultValue = defaultValueMap.get(dataRow.get("COLUMN_DEF"));
			boolean nullable = ((Number)dataRow.get("NULLABLE")).intValue() == DatabaseMetaData.columnNullable;
			boolean autoIncrement = false;// "YES".equalsIgnoreCase((String)dataRow.get("IS_AUTOINCREMENT"));
			
			columns[i] = new ColumnMetaData(tableName,columnName,caption,type,length,scale,
					defaultValue,nullable,autoIncrement);
		}
		
		Map<IndexInfo, Set<String>> indexColumnMap = new LinkedHashMap<>();
		int rowCount = UNKNOWN_ROW_COUNT;
		
		if(table.getType() == TableType.TABLE)
		{
			if((flags & INDICES) != 0)
			{
				Set<String> pkColumns = new HashSet<>();
				
				if(primaryKeyMap != null)
				{
					List<VirtualTableRow> pkRows = primaryKeyMap.get(tableName);
					if(pkRows != null && pkRows.size() > 0)
					{
						for(VirtualTableRow pkRow : pkRows)
						{
							pkColumns.add((String)pkRow.get("COLUMN_NAME"));
						}
						indexColumnMap.put(new IndexInfo("PRIMARY_KEY",IndexType.PRIMARY_KEY),
								pkColumns);
					}
				}
				
				if(indexMap != null)
				{
					List<VirtualTableRow> indexRows = indexMap.get(tableName);
					if(indexRows != null && indexRows.size() > 0)
					{
						for(VirtualTableRow pkRow : indexRows)
						{
							String indexName = (String)pkRow.get("INDEX_NAME");
							String columnName = (String)pkRow.get("COLUMN_NAME");
							if(indexName != null && columnName != null
									&& !pkColumns.contains(columnName))
							{
								boolean unique = "UNIQUE".equals(pkRow.get("UNIQUENESS"));
								IndexInfo info = new IndexInfo(indexName,unique ? IndexType.UNIQUE
										: IndexType.NORMAL);
								Set<String> columnNames = indexColumnMap.get(info);
								if(columnNames == null)
								{
									columnNames = new HashSet<>();
									indexColumnMap.put(info,columnNames);
								}
								columnNames.add(columnName);
							}
						}
					}
				}
			}
			
			if((flags & ROW_COUNT) != 0)
			{
				Object o = table.getClientProperty("num_rows");
				if(o instanceof Number)
				{
					rowCount = ((Number)o).intValue();
				}
			}
		}
		
		Index[] indices = new Index[indexColumnMap.size()];
		int i = 0;
		for(IndexInfo indexInfo : indexColumnMap.keySet())
		{
			Set<String> columnList = indexColumnMap.get(indexInfo);
			String[] indexColumns = columnList.toArray(new String[columnList.size()]);
			indices[i++] = new Index(indexInfo.name,indexInfo.type,indexColumns);
		}
		
		return new TableMetaData(table,columns,indices,rowCount);
	}
	
	@Override
	public StoredProcedure[] getStoredProcedures(ProgressMonitor monitor) throws DBException
	{
		monitor.beginTask("",ProgressMonitor.UNKNOWN);
		
		List<StoredProcedure> list = new ArrayList<>();
		
		DBConnection<?> connection = this.dataSource.openConnection();
		
		try
		{
			String schema = getSchema(this.dataSource);
			
			Map<String, List<Param>> paramMap = new HashMap<>();
			Map<String, Integer> returnTypeMap = new HashMap<>();
			
			Result rs = connection.query(
					"select * from all_procedures where owner=? and object_type in(?,?)",schema,
					"PROCEDURE","FUNCTION");
			try
			{
				
				while(rs.next() && !monitor.isCanceled())
				{
					String name = getQuotedString(rs.getString("OBJECT_NAME"));
					paramMap.put(name,new ArrayList<>());
				}
			}
			finally
			{
				rs.close();
			}
			
			if(!monitor.isCanceled())
			{
				rs = connection.query(
						"select * from all_arguments where owner=? order by object_name,position",
						schema);
				try
				{
					while(rs.next() && !monitor.isCanceled())
					{
						String procedureName = getQuotedString(rs.getString("OBJECT_NAME"));
						List<Param> params = paramMap.get(procedureName);
						if(params == null)
						{
							continue;
						}
						
						String paramName = rs.getString("ARGUMENT_NAME");
						
						String dataTypeName = rs.getString("DATA_TYPE");
						int sqlType = storedProcedure_toSQLType(dataTypeName);
						
						if(dataTypeName.equalsIgnoreCase("REF CURSOR"))
						{
							returnTypeMap.put(procedureName,Types.REF);
						}
						else if(paramName == null && rs.getInt("POSITION") == 0)
						{
							returnTypeMap.put(procedureName,sqlType);
						}
						else
						{
							DataType dataType = DataType.get(sqlType);
							
							String paramTypeName = rs.getString("IN_OUT");
							ParamType paramType;
							if(paramTypeName.equals("IN"))
							{
								paramType = ParamType.IN;
							}
							else if(paramTypeName.equals("OUT"))
							{
								paramType = ParamType.OUT;
							}
							else
							{
								paramType = ParamType.IN_OUT;
							}
							
							params.add(new Param(paramType,paramName,dataType));
						}
					}
				}
				finally
				{
					rs.close();
				}
			}
			
			for(String procedureName : paramMap.keySet())
			{
				List<Param> paramList = paramMap.get(procedureName);
				Param[] params = paramList.toArray(new Param[paramList.size()]);
				Integer returnType = returnTypeMap.get(procedureName);
				if(returnType == null)
				{
					list.add(new StoredProcedure(ReturnTypeFlavor.VOID,null,procedureName,"",params));
				}
				else if(returnType == Types.REF)
				{
					list.add(new StoredProcedure(ReturnTypeFlavor.RESULT_SET,null,procedureName,"",
							params));
				}
				else
				{
					list.add(new StoredProcedure(ReturnTypeFlavor.TYPE,DataType.get(returnType),
							procedureName,"",params));
				}
			}
		}
		finally
		{
			connection.close();
		}
		
		monitor.done();
		
		return list.toArray(new StoredProcedure[list.size()]);
	}
	
	
	private String getQuotedString(String string)
	{
		boolean delim = false;
		if(string != null)
		{
			
			int upper = 0, lower = 0;
			for(char ch : string.toCharArray())
			{
				if(Character.isLetter(ch))
				{
					if(Character.isUpperCase(ch))
					{
						upper++;
					}
					else
					{
						lower++;
					}
				}
				if(upper > 0 && lower > 0)
				{
					break;
				}
			}
			if(upper > 0 && lower > 0)
			{
				// probably case-sensitive
				delim = true;
			}
			else
			{
				try
				{
					SqlIdentifier.validateIdentifierString(string);
				}
				catch(SQLEngineInvalidIdentifier e)
				{
					delim = true;
				}
			}
			
		}
		
		if(delim)
		{
			string = DELIM + string + DELIM;
		}
		
		return string;
	}
	
	
	private int storedProcedure_toSQLType(String oracleTypeName)
	{
		if(oracleTypeName.equals("CHAR"))
		{
			return Types.CHAR;
		}
		else if(oracleTypeName.equals("VARCHAR2"))
		{
			return Types.VARCHAR;
		}
		else if(oracleTypeName.equals("NUMBER"))
		{
			return Types.DECIMAL;
		}
		else if(oracleTypeName.equals("LONG"))
		{
			return Types.LONGVARCHAR;
		}
		else if(oracleTypeName.equals("DATE"))
		{
			return Types.DATE;
		}
		else if(oracleTypeName.startsWith("TIMESTAMP"))
		{
			return Types.TIMESTAMP;
		}
		else if(oracleTypeName.startsWith("INTERVAL"))
		{
			return Types.TINYINT;
		}
		else if(oracleTypeName.equals("REF CURSOR"))
		{
			return Types.REF;
		}
		else if(oracleTypeName.equals("REAL"))
		{
			return Types.FLOAT;
		}
		else if(oracleTypeName.equals("CLOB"))
		{
			return Types.CLOB;
		}
		else if(oracleTypeName.equals("BLOB"))
		{
			return Types.BLOB;
		}
		else if(oracleTypeName.equals("FLOAT"))
		{
			return Types.FLOAT;
		}
		return Types.OTHER;
		
	}
	
	
	private Map<String, List<VirtualTableRow>> toMap(VirtualTable vt, String columnName)
	{
		Map<String, List<VirtualTableRow>> columnMap = new HashMap<>();
		int tableNameColumnIndex = vt.getColumnIndex(columnName);
		for(VirtualTableRow row : vt.rows())
		{
			CollectionUtils.accumulate(columnMap,(String)row.get(tableNameColumnIndex),row);
		}
		return columnMap;
	}
	
	
	@Override
	protected void createTable(JDBCConnection jdbcConnection, TableMetaData table)
			throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" (");
		
		ColumnMetaData[] columns = table.getColumns();
		for(int i = 0; i < columns.length; i++)
		{
			if(i > 0)
			{
				sb.append(", ");
			}
			
			ColumnMetaData column = columns[i];
			appendEscapedName(column.getName(),sb);
			sb.append(" ");
			appendColumnDefinition(column,sb);
		}
		
		for(Index index : table.getIndices())
		{
			sb.append(", ");
			appendIndexDefinition(table,index,sb);
		}
		
		sb.append(")");
		
		jdbcConnection.write(sb.toString());
		
		createOrUpdateSequencesAndTriggers(jdbcConnection,table);
	}
	
	
	@Override
	protected void alterTable(JDBCConnection jdbcConnection, TableMetaData table,
			TableMetaData existing) throws DBException, SQLException
	{
		super.alterTable(jdbcConnection,table,existing);
		
		createOrUpdateSequencesAndTriggers(jdbcConnection,table);
	}
	
	
	private void createOrUpdateSequencesAndTriggers(JDBCConnection jdbcConnection,
			TableMetaData table) throws DBException, SQLException
	{
		List<ColumnMetaData> autoIncrementColumns = new ArrayList<>();
		for(ColumnMetaData columnMeta : table.getColumns())
		{
			if(columnMeta.isAutoIncrement())
			{
				autoIncrementColumns.add(columnMeta);
			}
		}
		
		if(autoIncrementColumns.isEmpty())
		{
			return;
		}
		
		List<String> sequences = getSequenceNames(jdbcConnection);
		
		StringBuilder columns = new StringBuilder();
		StringBuilder into = new StringBuilder();
		
		int c = autoIncrementColumns.size();
		for(int i = 0; i < c; i++)
		{
			ColumnMetaData column = autoIncrementColumns.get(i);
			
			String sequence = getAutoIncrementSequenceName(table,column);
			
			if(!sequences.contains(sequence.toUpperCase()))
			{
				StringBuilder sb = new StringBuilder();
				sb.append("CREATE SEQUENCE ");
				sb.append(sequence);
				sb.append(" START WITH 1 INCREMENT BY 1");
				
				jdbcConnection.write(sb.toString());
			}
			
			if(i > 0)
			{
				columns.append(", ");
				into.append(", ");
			}
			columns.append(sequence);
			columns.append(".nextval");
			into.append(":NEW.");
			into.append(column.getName());
			
			StringBuilder sb = new StringBuilder();
			sb.append("CREATE OR REPLACE TRIGGER ");
			sb.append(getAutoIncrementTriggerName(table));
			sb.append(" BEFORE INSERT ON ");
			appendEscapedName(table.getTableInfo().getName(),sb);
			sb.append(" REFERENCING NEW AS NEW FOR EACH ROW BEGIN SELECT ");
			sb.append(columns);
			sb.append(" INTO ");
			sb.append(into);
			sb.append(" FROM DUAL; END;");
			
			jdbcConnection.write(sb.toString());
		}
	}
	
	
	@Override
	protected void addColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column, ColumnMetaData columnBefore, ColumnMetaData columnAfter)
			throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" ADD ");
		appendEscapedName(column.getName(),sb);
		sb.append(" ");
		appendColumnDefinition(column,sb);
		if(columnBefore == null)
		{
			sb.append(" FIRST");
		}
		else
		{
			sb.append(" AFTER ");
			appendEscapedName(columnBefore.getName(),sb);
		}
		
		jdbcConnection.write(sb.toString());
	}
	
	
	@Override
	protected void alterColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column, ColumnMetaData existing) throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" MODIFY ");
		appendEscapedName(existing.getName(),sb);
		sb.append(" ");
		appendColumnDefinition(column,sb);
		
		jdbcConnection.write(sb.toString());
	}
	

	@SuppressWarnings("incomplete-switch")
	@Override
	public boolean equalsType(ColumnMetaData clientColumn, ColumnMetaData dbColumn)
	{
		DataType clientType = clientColumn.getType();
		DataType dbType = dbColumn.getType();
		
		if(clientType == dbType)
		{
			switch(clientType)
			{
				case TINYINT:
				case SMALLINT:
				case INTEGER:
				case BIGINT:
				case REAL:
				case FLOAT:
				case DOUBLE:
				case DATE:
				case TIME:
				case TIMESTAMP:
				case CLOB:
				case BLOB:
				{
					return true;
				}
				
				case NUMERIC:
				case DECIMAL:
				{
					return clientColumn.getLength() == dbColumn.getLength()
							&& clientColumn.getScale() == dbColumn.getScale();
				}
				
				case CHAR:
				case VARCHAR:
				case LONGVARCHAR:
				{
					return clientColumn.getLength() == dbColumn.getLength();
				}
			}
		}
		
		Boolean match = getTypeMatch(clientColumn,dbColumn);
		if(match != null)
		{
			return match;
		}
		
		match = getTypeMatch(dbColumn,clientColumn);
		if(match != null)
		{
			return match;
		}
		
		return false;
	}
	

	@SuppressWarnings("incomplete-switch")
	private Boolean getTypeMatch(ColumnMetaData thisColumn, ColumnMetaData thatColumn)
	{
		DataType thisType = thisColumn.getType();
		DataType thatType = thatColumn.getType();
		
		switch(thisType)
		{
			case BOOLEAN:
			{
				return isNumeric(thatColumn,1);
			}
			
			case TINYINT:
			{
				return isNumeric(thatColumn,3);
			}
			
			case SMALLINT:
			{
				return isNumeric(thatColumn,5);
			}
			
			case INTEGER:
			{
				return thatType == DataType.BIGINT;
			}
			
			case BIGINT:
			{
				return isNumeric(thatColumn,38);
			}
			
			case DECIMAL:
			{
				return isNumeric(thatColumn,thisColumn.getLength(),thisColumn.getScale());
			}
			
			case REAL:
			{
				return isNumeric(thatColumn,63,-127);
			}
			
			case FLOAT:
			case DOUBLE:
			{
				return isNumeric(thatColumn,126,-127);
			}
			
			case VARCHAR:
			case LONGVARCHAR:
			{
				int length = thisColumn.getLength();
				if(length > 4000)
				{
					return thatType == DataType.CLOB;
				}
				else
				{
					return thatType == DataType.VARCHAR && length == thatColumn.getLength();
				}
			}
			
			case BINARY:
			case VARBINARY:
			case LONGVARBINARY:
			{
				int length = thisColumn.getLength();
				if(length > 2000)
				{
					return thatType == DataType.BLOB;
				}
				else
				{
					return thatType == DataType.VARBINARY;
				}
			}
			
			case TIME:
			{
				return thatType == DataType.DATE;
			}
		}
		
		return null;
	}
	
	
	private boolean isNumeric(ColumnMetaData column, int length)
	{
		return isNumeric(column,length,0);
	}
	

	@SuppressWarnings("incomplete-switch")
	private boolean isNumeric(ColumnMetaData column, int length, int scale)
	{
		switch(column.getType())
		{
			case NUMERIC:
			case INTEGER:
			{
				return column.getLength() == length && column.getScale() == scale;
			}
		}
		
		return false;
	}
	

	@SuppressWarnings("incomplete-switch")
	private void appendColumnDefinition(ColumnMetaData column, StringBuilder sb)
			throws SQLException
	{
		DataType type = column.getType();
		switch(type)
		{
			case TINYINT:
			{
				sb.append("NUMBER(3)");
			}
			break;
			
			case SMALLINT:
			{
				sb.append("NUMBER(5)");
			}
			break;
			
			case INTEGER:
			{
				sb.append("INTEGER");
			}
			break;
			
			case BIGINT:
			{
				sb.append("NUMBER(38)");
			}
			break;
			
			case REAL:
			{
				sb.append("REAL");
			}
			break;
			
			case FLOAT:
			{
				sb.append("FLOAT");
			}
			break;
			
			case DOUBLE:
			{
				sb.append("DOUBLE PRECISION");
			}
			break;
			
			case NUMERIC:
			case DECIMAL:
			{
				sb.append("NUMBER(");
				sb.append(column.getLength() + column.getScale());
				sb.append(",");
				sb.append(column.getScale());
				sb.append(")");
			}
			break;
			
			case DATE:
			case TIME:
			{
				sb.append("DATE");
			}
			break;
			
			case TIMESTAMP:
			{
				sb.append("TIMESTAMP");
			}
			break;
			
			case BOOLEAN:
			{
				sb.append("NUMBER(1)");
			}
			break;
			
			case CHAR:
			{
				sb.append("CHAR(");
				sb.append(column.getLength());
				sb.append(")");
			}
			break;
			
			case VARCHAR:
			case LONGVARCHAR:
			{
				int len = column.getLength();
				if(len > 4000)
				{
					sb.append("CLOB");
				}
				else
				{
					sb.append("VARCHAR2(");
					sb.append(len);
					sb.append(")");
				}
			}
			break;
			
			case CLOB:
			{
				sb.append("CLOB");
			}
			break;
			
			case BINARY:
			case VARBINARY:
			case LONGVARBINARY:
			{
				int len = column.getLength();
				if(len > 2000)
				{
					sb.append("BLOB");
				}
				else
				{
					sb.append("RAW(");
					sb.append(len);
					sb.append(")");
				}
			}
			break;
			
			case BLOB:
			{
				sb.append("BLOB");
			}
			break;
		}
		
		Object defaultValue = column.getDefaultValue();
		if(!(defaultValue == null && !column.isNullable()))
		{
			// Oracle doesn't allow variables in DML-Statements: see ORA-01027
			sb.append(" DEFAULT ");
			sb.append(toSQLStringValue(defaultValue));
		}
		
		if(column.isNullable())
		{
			sb.append(" NULL");
		}
		else
		{
			sb.append(" NOT NULL");
		}
	}
	
	private static DateFormat	sqlDateFormat	= new SimpleDateFormat("MM-dd-yyyy HH:mm:ss");
	
	
	private String toSQLStringValue(Object value)
	{
		if(value == null)
		{
			return "NULL";
		}
		
		if(value instanceof java.util.Date)
		{
			java.util.Date date = (java.util.Date)value;
			
			StringBuilder sb = new StringBuilder();
			sb.append("to_date('");
			sb.append(sqlDateFormat.format(date));
			sb.append("','MM-DD-YYYY HH24:MI:SS')");
			return sb.toString();
		}
		else if(value instanceof String)
		{
			char[] ch = ((String)value).toCharArray();
			StringBuffer sb = new StringBuffer(ch.length + 2);
			sb.append('\'');
			for(char c : ch)
			{
				if(c == '\'')
				{
					// quote: ' -> ''
					sb.append('\'');
				}
				sb.append(c);
			}
			sb.append('\'');
			return sb.toString();
		}
		
		return value.toString();
	}
	
	
	@Override
	protected void dropColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column) throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" DROP COLUMN ");
		appendEscapedName(column.getName(),sb);
		
		jdbcConnection.write(sb.toString());
		
		sb = new StringBuilder();
		sb.append("BEGIN EXECUTE IMMEDIATE 'DROP SEQUENCE ");
		appendEscapedName(getAutoIncrementSequenceName(table,column),sb);
		sb.append("'; EXCEPTION WHEN OTHERS THEN NULL; END");
		
		jdbcConnection.write(sb.toString());
	}
	
	
	private String getAutoIncrementSequenceName(TableMetaData table, ColumnMetaData column)
	{
		String name = table.getTableInfo().getName() + "_" + column.getName() + "_AI";
		if(name.length() > MAX_IDENTIFIER_LENGTH)
		{
			name = "AI_SEQ_" + System.currentTimeMillis();
		}
		return name;
	}
	
	
	private String getAutoIncrementTriggerName(TableMetaData table)
	{
		String name = table.getTableInfo().getName() + "_AI";
		if(name.length() > MAX_IDENTIFIER_LENGTH)
		{
			name = "AI_TRG_" + System.currentTimeMillis();
		}
		return name;
	}
	
	
	@Override
	protected void createIndex(JDBCConnection jdbcConnection, TableMetaData table, Index index)
			throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" ADD ");
		appendIndexDefinition(table,index,sb);
		
		jdbcConnection.write(sb.toString());
	}
	
	
	private void appendIndexDefinition(TableMetaData table, Index index, StringBuilder sb)
			throws DBException
	{
		sb.append("CONSTRAINT ");
		appendEscapedName(getIndexName(table,index),sb);
		sb.append(" ");
		
		switch(index.getType())
		{
			case PRIMARY_KEY:
			{
				sb.append("PRIMARY KEY");
			}
			break;
			
			case UNIQUE:
			{
				sb.append("UNIQUE");
			}
			break;
			
			case NORMAL:
			{
				throw new DBException(this.dataSource,
						"Only primary keys and unique indices are supported.");
			}
		}
		
		sb.append(" (");
		String[] columns = index.getColumns();
		for(int i = 0; i < columns.length; i++)
		{
			if(i > 0)
			{
				sb.append(", ");
			}
			appendEscapedName(columns[i],sb);
		}
		sb.append(")");
	}
	
	
	@Override
	protected void dropIndex(JDBCConnection jdbcConnection, TableMetaData table, Index index)
			throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		
		if(index.getType() == IndexType.PRIMARY_KEY)
		{
			sb.append(" DROP PRIMARY KEY");
		}
		else
		{
			sb.append(" DROP INDEX ");
			appendEscapedName(getIndexName(table,index),sb);
		}
		
		jdbcConnection.write(sb.toString());
	}
	
	
	private String getIndexName(TableMetaData table, Index index)
	{
		// In Oracle index names must be unique
		String name = table.getTableInfo().getName() + "_" + index.getName();
		if(name.length() > MAX_IDENTIFIER_LENGTH)
		{
			name = "IDX_" + System.currentTimeMillis();
		}
		return name;
	}
	
	
	/**
	 * 
	 * @param jdbcConnection
	 * @return List with upper case sequence names
	 * @throws DBException
	 */
	
	private List<String> getSequenceNames(JDBCConnection jdbcConnection) throws DBException
	{
		List<String> list = new ArrayList<>();
		
		Result result = jdbcConnection.query("SELECT sequence_name FROM user_sequences");
		while(result.next())
		{
			String name = result.getString("sequence_name");
			list.add(name.toUpperCase());
		}
		result.close();
		
		return list;
	}
}
