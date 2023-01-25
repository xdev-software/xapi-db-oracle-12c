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
import com.xdev.jadoth.sqlengine.SQL;
import com.xdev.jadoth.sqlengine.internal.SqlxAggregateCOLLECT_asString;


/**
 * The Class OracleSQL.
 * 
 * @author Thomas Muenz
 */
public class OracleSQL extends SQL
{
	
	/**
	 * W m_ concat.
	 * 
	 * @param expression
	 *            the expression
	 * @return the sqlx aggregate collec t_as string
	 */
	public static SqlxAggregateCOLLECT_asString WM_CONCAT(final Object expression)
	{
		return new SqlxAggregateCOLLECT_asString(expression);
	}
	
}
