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

import xdev.db.DBException;
import xdev.db.jdbc.JDBCDataSource;


public class Oracle12cJDBCDataSource extends JDBCDataSource<Oracle12cJDBCDataSource, Oracle12cDbms>
{
	public Oracle12cJDBCDataSource()
	{
		super(new Oracle12cDbms());
	}
	
	
	@Override
	public Parameter[] getDefaultParameters()
	{
		return new Parameter[]{HOST.clone(),PORT.clone(1521),USERNAME.clone("HR"),PASSWORD.clone(),
				CATALOG.clone("XE"),URL_EXTENSION.clone(),IS_SERVER_DATASOURCE.clone(),
				SERVER_URL.clone(),AUTH_KEY.clone()};
	}
	
	
	@Override
	protected Oracle12cConnectionInformation getConnectionInformation()
	{
		return new Oracle12cConnectionInformation(getHost(),getPort(),getUserName(),getPassword()
				.getPlainText(),getCatalog(),getUrlExtension(),getDbmsAdaptor());
	}
	
	
	@Override
	public Oracle12cJDBCConnection openConnectionImpl() throws DBException
	{
		return new Oracle12cJDBCConnection(this);
	}
	
	
	@Override
	public Oracle12cJDBCMetaData getMetaData() throws DBException
	{
		return new Oracle12cJDBCMetaData(this);
	}
	
	
	@Override
	public boolean canExport()
	{
		return false;
	}
}
