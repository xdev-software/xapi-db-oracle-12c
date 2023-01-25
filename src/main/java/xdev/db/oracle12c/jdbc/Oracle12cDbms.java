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
import com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor;
import com.xdev.jadoth.sqlengine.dbms.SQLExceptionParser;
import com.xdev.jadoth.sqlengine.interfaces.ConnectionProvider;
import com.xdev.jadoth.sqlengine.internal.DatabaseGateway;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;


/**
 * The Class Oracle11gDbms.
 */
public class Oracle12cDbms
		extends
		DbmsAdaptor.Implementation<Oracle12cDbms, Oracle12cDMLAssembler, Oracle12cDDLMapper, Oracle12cRetrospectionAccessor, Oracle12cSyntax>
{
	// /////////////////////////////////////////////////////////////////////////
	// constants //
	// ///////////////////
	
	/** The Constant MAX_VARCHAR_LENGTH. */
	protected static final int	MAX_VARCHAR_LENGTH		= 2000;
	
	protected static final char	IDENTIFIER_DELIMITER	= '"';
	
	
	// /////////////////////////////////////////////////////////////////////////
	// static methods //
	// /////////////////
	
	/**
	 * Single connection.
	 * 
	 * @param host
	 *            the host
	 * @param port
	 *            the port
	 * @param user
	 *            the user
	 * @param password
	 *            the password
	 * @param database
	 *            the database
	 * @return the connection provider
	 */
	public static ConnectionProvider<Oracle12cDbms> singleConnection(final String host,
			final int port, final String user, final String password, final String database, final String properties)
	{
		return new ConnectionProvider.Body<>(new Oracle12cConnectionInformation(host,
				port,user,password,database,properties, new Oracle12cDbms()));
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////
	/**
	 * Instantiates a new oracle11g dbms.
	 */
	public Oracle12cDbms()
	{
		this(new Oracle12cSQLExceptionParser());
	}
	
	
	/**
	 * Instantiates a new oracle11g dbms.
	 * 
	 * @param sqlExceptionParser
	 *            the sql exception parser
	 */
	public Oracle12cDbms(final SQLExceptionParser sqlExceptionParser)
	{
		super(sqlExceptionParser,false);
		this.setRetrospectionAccessor(new Oracle12cRetrospectionAccessor(this));
		this.setDMLAssembler(new Oracle12cDMLAssembler(this));
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// override methods //
	// ///////////////////
	
	/**
	 * @param host
	 * @param port
	 * @param user
	 * @param password
	 * @param catalog
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#createConnectionInformation(java.lang.String,
	 *      int, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public Oracle12cConnectionInformation createConnectionInformation(final String host,
			final int port, final String user, final String password, final String catalog, final String properties)
	{
		return new Oracle12cConnectionInformation(host,port,user,password,catalog,properties, this);
	}
	
	
	/**
	 * Oracle does not support any means of calculating table columns
	 * selectivity as far as it is known.<br>
	 * So this method does nothing and returns <tt>null</tt>
	 * 
	 * @param table
	 *            the table
	 * @return the object
	 */
	@Override
	public Object updateSelectivity(final SqlTableIdentity table)
	{
		return null;
	}
	
	
	/**
	 * @param dbc
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#initialize(com.xdev.jadoth.sqlengine.internal.DatabaseGateway)
	 */
	@Override
	public void initialize(final DatabaseGateway<Oracle12cDbms> dbc)
	{
		// No initialization needed
	}
	
	
	/**
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor.Implementation#supportsMERGE()
	 */
	@Override
	public boolean supportsMERGE()
	{
		return false;
	}
	
	
	@Override
	public boolean supportsOFFSET_ROWS()
	{
		return false;
	}
	
	
	/**
	 * @param bytes
	 * @param sb
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#assembleTransformBytes(byte[],
	 *      java.lang.StringBuilder)
	 */
	@Override
	public StringBuilder assembleTransformBytes(final byte[] bytes, final StringBuilder sb)
	{
		throw new RuntimeException("Oracle assembleTransformBytes() not implemented yet");
	}
	
	
	/**
	 * @param fullQualifiedTableName
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#rebuildAllIndices(java.lang.String)
	 */
	@Override
	public Object rebuildAllIndices(final String fullQualifiedTableName)
	{
		throw new RuntimeException("Oracle rebuildIndices() not implemented yet");
	}
	
	
	/**
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#getMaxVARCHARlength()
	 */
	@Override
	public int getMaxVARCHARlength()
	{
		return MAX_VARCHAR_LENGTH;
	}
	
	
	@Override
	public char getIdentifierDelimiter()
	{
		return IDENTIFIER_DELIMITER;
	}
}
