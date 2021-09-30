package xdev.db.oracle12c.jdbc;

/*-
 * #%L
 * SqlEngine Database Adapter Oracle 12c
 * %%
 * Copyright (C) 2003 - 2021 XDEV Software
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Lesser Public License for more details.
 * 
 * You should have received a copy of the GNU General Lesser Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/lgpl-3.0.html>.
 * #L%
 */


import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;

import oracle.jdbc.OracleCallableStatement;
import oracle.jdbc.OracleTypes;
import oracle.sql.BLOB;
import oracle.sql.CLOB;
import xdev.db.ConnectionWrapper;
import xdev.db.DBDataSource;
import xdev.db.DBException;
import xdev.db.DBPager;
import xdev.db.Result;
import xdev.db.StoredProcedure;
import xdev.db.StoredProcedure.Param;
import xdev.db.StoredProcedure.ParamType;
import xdev.db.StoredProcedure.ReturnTypeFlavor;
import xdev.db.jdbc.JDBCConnection;
import xdev.db.jdbc.JDBCResult;
import xdev.db.sql.SELECT;
import xdev.util.MathUtils;


public class Oracle12cJDBCConnection extends JDBCConnection<Oracle12cJDBCDataSource, Oracle12cDbms>
{
	public Oracle12cJDBCConnection(Oracle12cJDBCDataSource dataSource)
	{
		super(dataSource);
	}
	
	
	@Override
	public int getQueryRowCount(String select) throws DBException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT COUNT(*) FROM ("); //$NON-NLS-1$
		sb.append(select);
		sb.append(")"); //$NON-NLS-1$
		
		Result result = query(sb.toString());
		try
		{
			result.next();
			int rowCount = result.getInt(0);
			return rowCount;
		}
		finally
		{
			result.close();
		}
	}
	
	
	@SuppressWarnings("deprecation")
	@Override
	protected void prepareParams(Connection connection, Object[] params) throws DBException
	{
		super.prepareParams(connection,params);
		
		if(params != null)
		{
			while(connection instanceof ConnectionWrapper)
			{
				connection = ((ConnectionWrapper)connection).getActualConnection();
			}
			
			for(int i = 0; i < params.length; i++)
			{
				if(params[i] instanceof Blob)
				{
					Blob blob = (Blob)params[i];
					try
					{
						BLOB oracleBlob = BLOB.createTemporary(connection,false,
								BLOB.DURATION_SESSION);
						// use deprecated putBytes, setBytes doesn't work
						// https://forums.oracle.com/forums/thread.jspa?threadID=1323569
						oracleBlob.putBytes(1,blob.getBytes(1,(int)blob.length()));
						params[i] = oracleBlob;
					}
					catch(SQLException e)
					{
						throw new DBException(getDataSource(),e);
					}
				}
				else if(params[i] instanceof Clob)
				{
					Clob clob = (Clob)params[i];
					try
					{
						CLOB oracleClob = CLOB.createTemporary(connection,false,
								CLOB.DURATION_SESSION);
						oracleClob
								.putChars(1,clob.getSubString(1,(int)clob.length()).toCharArray());
						params[i] = oracleClob;
					}
					catch(SQLException e)
					{
						throw new DBException(getDataSource(),e);
					}
				}
			}
		}
	}
	
	
	@Override
	public void call(StoredProcedure procedure, Object... params) throws DBException
	{
		try
		{
			Connection connection = getConnection();
			
			prepareParams(connection,params);
			
			procedure.prepareCall(params);
			int pc = procedure.getParamCount();
			
			ReturnTypeFlavor returnTypeFlavor = procedure.getReturnTypeFlavor();
			
			StringBuffer query = new StringBuffer();
			boolean hasReturnType = false;
			if(returnTypeFlavor == ReturnTypeFlavor.TYPE
					|| returnTypeFlavor == ReturnTypeFlavor.RESULT_SET)
			{
				hasReturnType = true;
				createQueryStringWithReturnType(procedure,pc,hasReturnType,query);
				
			}
			else
			{
				hasReturnType = false;
				createQueryStringWithOutReturnType(procedure,pc,hasReturnType,query);
			}
			
			CallableStatement statement = connection.prepareCall(query.toString());
			int pi = 1;
			if(hasReturnType)
			{
				// set return value type it is always parameter 1
				if(returnTypeFlavor == ReturnTypeFlavor.RESULT_SET)
				{
					statement.registerOutParameter(pi++,OracleTypes.CURSOR);
				}
				else
				{
					statement.registerOutParameter(pi++,-1);
				}
			}
			
			for(int i = 0; i < pc; i++, pi++)
			{
				Param param = procedure.getParam(i);
				ParamType type = param.getParamType();
				
				if(type == ParamType.IN || type == ParamType.IN_OUT)
				{
					if(param.getValue() instanceof String)
					{
						statement.setString(pi,(String)param.getValue());
						
					}
					
					statement.setObject(pi,param.getValue());
				}
				
				if(type == ParamType.OUT || type == ParamType.IN_OUT)
				{
					int jdbcType = param.getDataType().getSqlType().getJdbcType();
					statement.registerOutParameter(pi,jdbcType);
				}
			}
			
			pi = 1;
			Object returnValue = null;
			statement.execute();
			if(returnTypeFlavor == ReturnTypeFlavor.RESULT_SET)
			{				
				ResultSet cursor = ((OracleCallableStatement)statement).getCursor(1);
				/*
				 * handed over to StoredProcedure, which is an AutoCloseable
				 * itself
				 */
				@SuppressWarnings("resource")
				JDBCResult result = new JDBCResult(cursor);
				result.setDataSource(this.dataSource);
				returnValue = result;
			}
			else
			{
				if(returnTypeFlavor == ReturnTypeFlavor.TYPE)
				{
					returnValue = statement.getObject(pi++);
				}
			}
			
			for(int i = 0; i < pc; i++, pi++)
			{
				Param param = procedure.getParam(i);
				ParamType type = param.getParamType();
				
				if(type == ParamType.OUT || type == ParamType.IN_OUT)
				{
					Object result = statement.getObject(pi);
					if(result != null)
					{
						if(result instanceof ResultSet)
						{
							JDBCResult resultSet = new JDBCResult((ResultSet)result);
							param.setValue(resultSet);
							
						}
						else
						{
							param.setValue(result);
						}
					}
					else
					{
						param.setValue(null);
					}
				}
			}
			
			procedure.setReturnValue(returnValue);
		}
		catch(DBException e)
		{
			throw e;
		}
		catch(Exception e)
		{
			throw new DBException(this.dataSource,e);
		}
		
	}
	
	
	private void createQueryStringWithOutReturnType(StoredProcedure procedure, int pc,
			boolean hasReturnType, StringBuffer query)
	{
		query.append("{"); //$NON-NLS-1$
		if(hasReturnType)
		{
			query.append("? = "); //$NON-NLS-1$
		}
		query.append("call "); //$NON-NLS-1$
		query.append(procedure.getName());
		query.append("("); //$NON-NLS-1$
		for(int i = 0; i < pc; i++)
		{
			if(i > 0)
			{
				query.append(","); //$NON-NLS-1$
			}
			query.append("?"); //$NON-NLS-1$
		}
		query.append(")}"); //$NON-NLS-1$
		
	}
	
	
	private void createQueryStringWithReturnType(StoredProcedure procedure, int pc,
			boolean hasReturnType, StringBuffer query)
	{
		
		query.append("BEGIN \"" + procedure.getName() + "\""); //$NON-NLS-1$//$NON-NLS-2$
		
		query.append("("); //$NON-NLS-1$
		
		if(hasReturnType)
		{
			query.append("?"); //$NON-NLS-1$
		}
		
		for(int i = 0; i < pc; i++)
		{
			if(i == 0)
			{
				if(hasReturnType)
				{
					query.append(","); //$NON-NLS-1$
				}
			}
			else
			{
				query.append(","); //$NON-NLS-1$
			}
			query.append("?"); //$NON-NLS-1$
		}
		query.append("); END;"); //$NON-NLS-1$
	}
	
	
	@Override
	public DBPager getPager(int rowsPerPage, SELECT select, Object... params) throws DBException
	{
		return new OraclePager(rowsPerPage,select,params);
	}
	
	
	
	private class OraclePager implements DBPager
	{
		final String	select;
		final Object[]	params;
		
		int				rowsPerPage;
		int				totalRows;
		int				maxPageIndex;
		int				currentPageIndex	= -1;
		int				currentRowIndex		= -1;
		
		Result			lastResult;
		boolean			closed				= false;
		
		
		OraclePager(int rowsPerPage, SELECT select, Object[] params) throws DBException
		{
			decorateDelegate(select,Oracle12cJDBCConnection.this.gateway);
			
			Integer offset = select.getOffsetSkipCount();
			Integer limit = select.getFetchFirstRowCount();
			
			if(!Oracle12cJDBCConnection.this.gateway.getDbmsAdaptor().supportsOFFSET_ROWS() && offset != null && offset > 0
					&& limit != null && limit > 0)
			{
				limit += offset;
				select.FETCH_FIRST(limit);
			}
			
			this.select = select.toString();
			this.params = params;
			
			String sql = "SELECT COUNT(*) FROM (" + this.select + ")"; //$NON-NLS-1$ //$NON-NLS-2$
			Result r = query(sql,offset,limit,params);
			r.next();
			this.totalRows = r.getInt(0);
			
			setRowsPerPageValues(rowsPerPage);
		}
		
		
		@Override
		public DBDataSource<?> getDataSource()
		{
			return Oracle12cJDBCConnection.this.getDataSource();
		}
		
		
		@Override
		public Result setRowsPerPage(int rowsPerPage) throws DBException
		{
			setRowsPerPageValues(rowsPerPage);
			
			if(this.currentRowIndex == -1)
			{
				return nextPage();
			}
			
			return gotoPage(rowsPerPage == 0 ? 0 : this.currentRowIndex / rowsPerPage);
		}
		
		
		private void setRowsPerPageValues(int rowsPerPage)
		{
			this.rowsPerPage = rowsPerPage;
			
			if(rowsPerPage > 0)
			{
				this.maxPageIndex = this.totalRows / rowsPerPage;
				if(this.totalRows % rowsPerPage == 0)
				{
					this.maxPageIndex--;
				}
			}
			else
			{
				this.maxPageIndex = 0;
			}
		}
		
		
		@Override
		public int getRowsPerPage()
		{
			return this.rowsPerPage;
		}
		
		
		@Override
		public int getTotalRows()
		{
			return this.totalRows;
		}
		
		
		@Override
		public int getCurrentPageIndex()
		{
			return this.currentPageIndex;
		}
		
		
		@Override
		public int getMaxPageIndex()
		{
			return this.maxPageIndex;
		}
		
		
		@Override
		public boolean hasNextPage()
		{
			return Math.max(this.currentRowIndex,0) + this.rowsPerPage < this.totalRows;
		}
		
		
		@Override
		public boolean hasPreviousPage()
		{
			return this.currentRowIndex > 0;
		}
		
		
		@Override
		public Result nextPage() throws DBException
		{
			if(!hasNextPage())
			{
				return null;
			}
			
			if(this.currentRowIndex == -1 || this.currentPageIndex == -1)
			{
				this.currentRowIndex = this.currentPageIndex = 0;
			}
			else
			{
				this.currentRowIndex += this.rowsPerPage;
				this.currentPageIndex++;
			}
			
			return getResult();
		}
		
		
		@Override
		public Result previousPage() throws DBException
		{
			if(!hasPreviousPage())
			{
				return null;
			}
			
			this.currentRowIndex -= this.rowsPerPage;
			if(this.currentRowIndex < 0)
			{
				this.currentRowIndex = 0;
			}
			this.currentPageIndex--;
			
			return getResult();
		}
		
		
		@Override
		public Result firstPage() throws DBException
		{
			if(!hasPreviousPage())
			{
				return null;
			}
			
			this.currentRowIndex = this.currentPageIndex = 0;
			
			return getResult();
		}
		
		
		@Override
		public Result lastPage() throws DBException
		{
			if(!hasNextPage())
			{
				return null;
			}
			
			this.currentRowIndex = this.maxPageIndex * this.rowsPerPage;
			this.currentPageIndex = this.maxPageIndex;
			
			return getResult();
		}
		
		
		@Override
		public Result gotoPage(int page) throws DBException
		{
			MathUtils.checkRange(page,0,this.maxPageIndex);
			
			this.currentRowIndex = page * this.rowsPerPage;
			this.currentPageIndex = page;
			
			return getResult();
		}
		
		
		@Override
		public Result gotoRow(int row) throws DBException
		{
			MathUtils.checkRange(row,0,this.totalRows - 1);
			
			this.currentRowIndex = row;
			this.currentPageIndex = row / this.rowsPerPage;
			
			return getResult();
		}
		
		
		private Result getResult() throws DBException
		{
			if(this.lastResult != null)
			{
				this.lastResult.close();
			}
			
			int min = this.currentRowIndex + 1;
			int max = min + this.rowsPerPage;
			String sql = "SELECT * FROM ( SELECT a.*, rownum r__ FROM (" + this.select //$NON-NLS-1$
					+ ") a WHERE rownum < " + max + ") aa WHERE aa.r__ >= " + min; //$NON-NLS-1$ //$NON-NLS-2$
			return this.lastResult = query(sql,this.params);
		}
		
		
		@Override
		public void close() throws DBException
		{			
			if(this.lastResult != null)
			{
				this.lastResult.close();
			}

			this.closed = true;
		}
		
		
		@Override
		public boolean isClosed() throws DBException
		{
			return this.closed;
		}
	}
	
	
	@SuppressWarnings("nls")
	@Override
	public void createTable(String tableName, String primaryKey, Map<String, String> columnMap,
			boolean isAutoIncrement, Map<String, String> foreignKeys) throws Exception
	{
		Connection connection = super.getConnection();
		
		try
		{
			if(!checkIfTableExists(connection,tableName))
			{
				if(!columnMap.containsKey(primaryKey))
				{
					columnMap.put(primaryKey,"INTEGER");
				}
				StringBuffer createStatement = null;
				
				createStatement = new StringBuffer("CREATE TABLE " + tableName + "(" + primaryKey
						+ " " + columnMap.get(primaryKey) + " NOT NULL,");
				
				for(String keySet : columnMap.keySet())
				{
					if(!keySet.equals(primaryKey))
					{
						createStatement.append(keySet + " " + columnMap.get(keySet) + ",");
					}
				}
				
				createStatement.append(" CONSTRAINT cstr_" + tableName + " PRIMARY KEY ("
						+ primaryKey + "))");
				
				executeSql(connection,createStatement.toString());
				
				if(isAutoIncrement)
				{
					executeSql(connection,"Create sequence " + primaryKey
							+ "_pml_seq start with 1 increment by 1 nomaxvalue");
				}
			}
		}
		catch(Exception e)
		{
			throw e;
		}
		finally
		{
			connection.close();
		}
	}
	
	
	private boolean checkIfTableExists(Connection connection, String tableName) throws Exception
	{
		Statement statement = connection.createStatement();
		String sql = "SELECT * FROM user_tables WHERE table_name='" + tableName + "'"; //$NON-NLS-1$ //$NON-NLS-2$
		
		ResultSet resultSet = null;
		try
		{
			statement.execute(sql);
			resultSet = statement.getResultSet();
		}
		catch(Exception e)
		{
			if(resultSet != null)
			{
				resultSet.close();
			}
			statement.close();
			throw e;
		}
		
		if(resultSet != null)
		{
			while(resultSet.next())
			{
				resultSet.close();
				statement.close();
				return true;
				
			}
			resultSet.close();
			
		}
		statement.close();
		return false;
	}
	
	
	private void executeSql(Connection connection, String sqlStatement) throws SQLException,
			Exception
	{
		if(log.isDebugEnabled())
		{
			log.debug("SQL Statement to create a table: " + sqlStatement); //$NON-NLS-1$
		}
		
		Statement statement = connection.createStatement();
		try
		{
			statement.execute(sqlStatement);
		}
		catch(Exception e)
		{
			throw e;
		}
		finally
		{
			statement.close();
		}
	}
	
	
	@Override
	public Date getServerTime() throws DBException, ParseException
	{
		String selectTime = "select to_char(systimestamp, 'IYYY-MM-DD HH24:MI:SS.FF') from dual"; //$NON-NLS-1$
		return super.getServerTime(selectTime);
	}
	
}
