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

import java.sql.SQLException;

import com.xdev.jadoth.sqlengine.dbms.SQLExceptionParser;
import com.xdev.jadoth.sqlengine.exceptions.SQLEngineException;
import com.xdev.jadoth.sqlengine.exceptions.SQLEngineTableNotFoundException;



/**
 * The Class Oracle11gSQLExceptionParser.
 * 
 * @author Thomas Muenz
 */
public class Oracle12cSQLExceptionParser implements SQLExceptionParser
{
	/** The Constant SQLCODE_TableOrViewNotFound. */
	public static final short	SQLCODE_TableOrViewNotFound	= 942;
	
	/**
	 * @param e
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.SQLExceptionParser#parseSQLException(java.sql.SQLException)
	 */
	@Override
	public SQLEngineException parseSQLException(final SQLException e)
	{
		switch(e.getErrorCode())
		{
			case SQLCODE_TableOrViewNotFound:
				return new SQLEngineTableNotFoundException(e);

			default:
				return new SQLEngineException(e);
		}
	}
	
	
	/**
	 * Instantiates a new oracle11g sql exception parser.
	 */
	public Oracle12cSQLExceptionParser()
	{
		super();
	}
	
}
