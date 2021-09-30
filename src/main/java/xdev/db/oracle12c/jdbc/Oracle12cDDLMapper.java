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


import static com.xdev.jadoth.sqlengine.SQL.Punctuation.par;

import java.util.Hashtable;

import com.xdev.jadoth.sqlengine.SQL;
import com.xdev.jadoth.sqlengine.SQL.DATATYPE;
import com.xdev.jadoth.sqlengine.SQL.INDEXTYPE;
import com.xdev.jadoth.sqlengine.dbms.standard.StandardDDLMapper;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;


/**
 * The Class Oracle11gDDLMapper.
 * 
 * @author Thomas Muenz
 */
public class Oracle12cDDLMapper extends StandardDDLMapper<Oracle12cDbms>
{
	// /////////////////////////////////////////////////////////////////////////
	// constants //
	// ///////////////////
	
	/** The Constant dictionaryTypes. */
	private static final Hashtable<String, DATATYPE>	dictionaryTypes	= createDictionaryTypes();
	
	
	/**
	 * Creates the dictionary types.
	 * 
	 * @return the hashtable
	 */
	private static final Hashtable<String, DATATYPE> createDictionaryTypes()
	{
		final Hashtable<String, DATATYPE> c = new Hashtable<>(20);
		
		c.put("BOOLEAN"  ,SQL.DATATYPE.BOOLEAN);
		
		c.put("SMALLINT" ,SQL.DATATYPE.SMALLINT);
		c.put("INTEGER"  ,SQL.DATATYPE.INT);
		c.put("BIGINT"   ,SQL.DATATYPE.BIGINT);
		
		c.put("FLOAT"    ,SQL.DATATYPE.FLOAT);
		c.put("NUMBER"   ,SQL.DATATYPE.NUMERIC);
		c.put("NUMERIC"  ,SQL.DATATYPE.NUMERIC);
		
		c.put("TIMESTAMP",SQL.DATATYPE.TIMESTAMP);

		c.put("VARCHAR2" ,SQL.DATATYPE.VARCHAR);
		c.put("CHAR"     ,SQL.DATATYPE.CHAR);
		
		return c;
	}
	
	/** The Constant dictionaryIndexTypes. */
	private static final Hashtable<String, INDEXTYPE>	dictionaryIndexTypes	= createDictionaryIndexTypes();
	
	
	/**
	 * Creates the dictionary index types.
	 * 
	 * @return the hashtable
	 */
	private static final Hashtable<String, INDEXTYPE> createDictionaryIndexTypes()
	{
		final Hashtable<String, INDEXTYPE> c = new Hashtable<>(10);
		c.put("NORMAL",INDEXTYPE.NORMAL);
		c.put(SQL.LANG.BITMAP,INDEXTYPE.BITMAP);
		c.put(SQL.LANG.UNIQUE,INDEXTYPE.UNIQUE);
		return c;
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	
	/**
	 * Instantiates a new oracle11g ddl mapper.
	 * 
	 * @param dbmsAdaptor
	 *            the dbms adaptor
	 */
	protected Oracle12cDDLMapper(final Oracle12cDbms dbmsAdaptor)
	{
		super(dbmsAdaptor);
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// override methods //
	// ///////////////////
	
	/**
	 * Map data type.
	 * 
	 * @param dataTypeString
	 *            the data type string
	 * @return the dATATYPE
	 * @return
	 * @see net.jadoth.sqlengine.dbmsadaptor.DbmsDDLMapper.Implementation#mapDataType(java.lang.String)
	 */
	@Override
	public DATATYPE mapDataType(final String dataTypeString)
	{
		final DATATYPE mappedType = dictionaryTypes
				.get(this.parseToFirstParentheis(dataTypeString));
		return mappedType != null ? mappedType : SQL.DATATYPE.VARCHAR;
	}
	
	
	/**
	 * Map index type.
	 * 
	 * @param indexTypeString
	 *            the index type string
	 * @return the iNDEXTYPE
	 * @return
	 * @see net.jadoth.sqlengine.dbmsadaptor.DbmsDDLMapper.Implementation#mapIndexType(java.lang.String)
	 */
	@Override
	public INDEXTYPE mapIndexType(final String indexTypeString)
	{
		final INDEXTYPE mappedType = dictionaryIndexTypes.get(indexTypeString);
		return mappedType != null ? mappedType : INDEXTYPE.NORMAL;
	}
	
	
	/**
	 * Lookup ddbms data type mapping.
	 * 
	 * @param type
	 *            the type
	 * @param table
	 *            the table
	 * @return the string
	 * @return
	 * @see net.jadoth.sqlengine.dbmsadaptor.DbmsDDLMapper.Implementation#lookupDdbmsDataTypeMapping(com.xdev.jadoth.sqlengine.SQL.DATATYPE,
	 *      com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity)
	 */
	@Override
	public String lookupDdbmsDataTypeMapping(final DATATYPE type, final SqlTableIdentity table)
	{
		switch(type)
		{
			case VARCHAR:
				return "VARCHAR2";
			default:
				return super.getDataTypeDDLString(type,table);
		}
	}
	
	
	/**
	 * Parses the to first parenthesis.
	 * 
	 * @param s
	 *            the s
	 * @return the string
	 */
	protected String parseToFirstParentheis(final String s)
	{
		final int firstParIndex = s.indexOf(par);
		if(firstParIndex == -1)
		{
			return s;
		}
		return s.substring(0,firstParIndex);
	}
	
}
