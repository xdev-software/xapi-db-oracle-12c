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


import static com.xdev.jadoth.sqlengine.SQL.LANG.AND;
import static com.xdev.jadoth.sqlengine.SQL.LANG.COUNT;
import static com.xdev.jadoth.sqlengine.SQL.LANG.FROM;
import static com.xdev.jadoth.sqlengine.SQL.LANG.GROUP_BY;
import static com.xdev.jadoth.sqlengine.SQL.LANG.HAVING;
import static com.xdev.jadoth.sqlengine.SQL.LANG.INNER_JOIN;
import static com.xdev.jadoth.sqlengine.SQL.LANG.LEFT_JOIN;
import static com.xdev.jadoth.sqlengine.SQL.LANG.MIN;
import static com.xdev.jadoth.sqlengine.SQL.LANG.ON;
import static com.xdev.jadoth.sqlengine.SQL.LANG.SELECT;
import static com.xdev.jadoth.sqlengine.SQL.LANG.WHERE;
import static com.xdev.jadoth.sqlengine.SQL.LANG._AS_;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.NEW_LINE;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.TAB;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation._;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation._eq_;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.apo;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.comma;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.comma_;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.dot;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.par;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.rap;

import java.math.BigDecimal;

import com.xdev.jadoth.sqlengine.SQL.DATATYPE;
import com.xdev.jadoth.sqlengine.SQL.INDEXTYPE;
import com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor;
import com.xdev.jadoth.sqlengine.exceptions.SQLEngineException;
import com.xdev.jadoth.sqlengine.interfaces.SqlExecutor;
import com.xdev.jadoth.sqlengine.internal.SqlField;
import com.xdev.jadoth.sqlengine.internal.tables.SqlIndex;
import com.xdev.jadoth.sqlengine.internal.tables.SqlPrimaryKey;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;
import com.xdev.jadoth.sqlengine.util.ResultTable;



/**
 * The Class Oracle11gRetrospectionAccessor.
 */
public class Oracle12cRetrospectionAccessor extends
		DbmsRetrospectionAccessor.Implementation<Oracle12cDbms>
{
	
	// /////////////////////////////////////////////////////////////////////////
	// static fields //
	// ////////////////
	/** The Constant TABLE_user_tables. */
	public static final String				TABLE_user_tables					= "user_tables";
	
	/** The Constant TABLE_all_tables. */
	public static final String				TABLE_all_tables					= "all_tables";
	
	/** The Constant TABLE_dba_tables. */
	public static final String				TABLE_dba_tables					= "dba_tables";
	
	/** The Constant TABLE_user_tab_columns. */
	public static final String				TABLE_user_tab_columns				= "user_tab_columns";
	
	/** The Constant TABLE_user_indexes. */
	public static final String				TABLE_user_indexes					= "user_indexes";
	
	/** The Constant TABLE_user_ind_columns. */
	public static final String				TABLE_user_ind_columns				= "user_ind_columns";
	
	/** The Constant TABLE_user_constraints. */
	public static final String				TABLE_user_constraints				= "user_constraints";
	
	/** The Constant COLUMN_table_name. */
	public static final String				COLUMN_table_name					= "table_name";
	
	/** The Constant SYSTEMTABLE_TABLES. */
	public static final SqlTableIdentity	SYSTEMTABLE_TABLES					= new SqlTableIdentity(
																						null,
																						TABLE_user_tables,
																						"UT");
	
	/** The Constant SYSTEMTABLE_COLUMNS. */
	public static final SqlTableIdentity	SYSTEMTABLE_COLUMNS					= new SqlTableIdentity(
																						null,
																						TABLE_user_tab_columns,
																						"UTC");
	
	/** The Constant SYSTEMTABLE_INDICES. */
	public static final SqlTableIdentity	SYSTEMTABLE_INDICES					= new SqlTableIdentity(
																						null,
																						TABLE_user_indexes,
																						"UI");
	
	/** The Constant RTCOL_COLNAME. */
	protected static final String			RTCOL_COLNAME						= "ColName";
	
	/** The Constant RTCOL_COLTYPE. */
	protected static final String			RTCOL_COLTYPE						= "ColType";
	
	/** The Constant RTCOL_COLLENG. */
	protected static final String			RTCOL_COLLENG						= "ColLeng";
	
	/** The Constant RTCOL_COLPREC. */
	protected static final String			RTCOL_COLPREC						= "ColPrec";
	
	/** The Constant RTCOL_COLSCAL. */
	protected static final String			RTCOL_COLSCAL						= "ColScal";
	
	/** The Constant RTCOL_COLNULL. */
	protected static final String			RTCOL_COLNULL						= "ColNull";
	
	/** The Constant RTCOL_COLUNIQ. */
	protected static final String			RTCOL_COLUNIQ						= "ColUniq";
	
	/** The Constant RTCOL_COLDEFL. */
	protected static final String			RTCOL_COLDEFL						= "ColDefl";
	
	/** The Constant RTIDX_COLNAME. */
	protected static final int				RTIDX_COLNAME						= 0;
	
	/** The Constant RTIDX_COLTYPE. */
	protected static final int				RTIDX_COLTYPE						= 1;
	
	/** The Constant RTIDX_COLLENG. */
	protected static final int				RTIDX_COLLENG						= 2;
	
	/** The Constant RTIDX_COLPREC. */
	protected static final int				RTIDX_COLPREC						= 3;
	
	/** The Constant RTIDX_COLSCAL. */
	protected static final int				RTIDX_COLSCAL						= 4;
	
	/** The Constant RTIDX_COLNULL. */
	protected static final int				RTIDX_COLNULL						= 5;
	
	/** The Constant RTIDX_COLUNIQ. */
	protected static final int				RTIDX_COLUNIQ						= 6;
	
	/** The Constant RTIDX_COLDEFL. */
	protected static final int				RTIDX_COLDEFL						= 7;
	
	/** The Constant RTCOL_IDXNAME. */
	protected static final String			RTCOL_IDXNAME						= "IdxName";
	
	/** The Constant RTCOL_IDXTYPE. */
	protected static final String			RTCOL_IDXTYPE						= "IdxType";
	
	/** The Constant RTCOL_IDXCOLS. */
	protected static final String			RTCOL_IDXCOLS						= "IdxCols";
	
	/** The Constant RTCOL_IDXUNIQ. */
	protected static final String			RTCOL_IDXUNIQ						= "IdxUniq";
	
	/** The Constant RTCOL_IDXPRIM. */
	protected static final String			RTCOL_IDXPRIM						= "IdxPrim";
	
	/** The Constant RTIDX_IDXNAME. */
	protected static final int				RTIDX_IDXNAME						= 0;
	
	/** The Constant RTIDX_IDXTYPE. */
	protected static final int				RTIDX_IDXTYPE						= 1;
	
	/** The Constant RTIDX_IDXUNIQ. */
	protected static final int				RTIDX_IDXUNIQ						= 2;
	
	/** The Constant RTIDX_IDXPRIM. */
	protected static final int				RTIDX_IDXPRIM						= 3;
	
	/** The Constant RTIDX_IDXCOLS. */
	protected static final int				RTIDX_IDXCOLS						= 4;
	
	/** The Constant COL. */
	private static final String				COL									= "COL";
	
	/** The Constant IND. */
	private static final String				IND									= "IND";
	
	/** The Constant IC. */
	private static final String				IC									= "IC";
	
	/** The Constant CON. */
	private static final String				CON									= "CON";
	
	/** The Constant COLd. */
	private static final String				COLd								= COL + dot;
	
	/** The Constant INDd. */
	private static final String				INDd								= IND + dot;
	
	/** The Constant ICd. */
	private static final String				ICd									= IC + dot;
	
	/** The Constant CONd. */
	private static final String				CONd								= CON + dot;
	
	/** The Constant __. */
	private static final String				__									= _ + "" + _;
	
	/** The Constant QueryLoadTables_WHERE_Table_LIKE_. */
	private static final String				QueryLoadTables_WHERE_Table_LIKE_	= SELECT
																						+ NEW_LINE
																						+ __
																						+ "sys_context('userenv', 'current_schema')"
																						+ _AS_
																						+ Column_TABLE_SCHEMA
																						+ comma
																						+ NEW_LINE
																						+ __
																						+ COLUMN_table_name
																						+ _AS_
																						+ Column_TABLE_NAME
																						+ NEW_LINE
																						+
																						
																						FROM
																						+ _
																						+ TABLE_user_tables
																						+ NEW_LINE +
																						
																						WHERE + _;
	
	
	// /////////////////////////////////////////////////////////////////////////
	// static methods //
	// ///////////////////
	/**
	 * Parses the boolean from string.
	 * 
	 * @param s
	 *            the s
	 * @return the boolean
	 */
	private Boolean parseBooleanFromString(final String s)
	{
		if(s == null)
		{
			return null;
		}
		if(s.equals("Y"))
		{
			return true;
		}
		return false;
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	/**
	 * Instantiates a new oracle11g retrospection accessor.
	 * 
	 * @param dbmsadaptor
	 *            the dbmsadaptor
	 */
	public Oracle12cRetrospectionAccessor(final Oracle12cDbms dbmsadaptor)
	{
		super(dbmsadaptor);
	}
	
	
	/**
	 * @param table
	 * @return
	 * @throws SQLEngineException
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor.Implementation#loadColumns(com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity)
	 */
	@Override
	public SqlField[] loadColumns(final SqlTableIdentity table) throws SQLEngineException
	{
		final String selectInformationSchemaColumns = createSelect_INFORMATION_SCHEMA_COLUMNS(table);
		final ResultTable rt = new ResultTable(getDbmsAdaptor().getDatabaseGateway().execute(
				SqlExecutor.query,selectInformationSchemaColumns));
		final Oracle12cDDLMapper ddlMapper = this.getDbmsAdaptor().getDdlMapper();
		
		final int rowCount = rt.getRowCount();
		final SqlField[] columns = new SqlField[rowCount];
		
		String name = null;
		DATATYPE type = null;
		Boolean nullable = true;
		Object defaultValue = null;
		BigDecimal length = null;
		BigDecimal precision = null;
		BigDecimal scale = null;
		
		for(int i = 0; i < rowCount; i++)
		{
			name = rt.getValue(i,RTIDX_COLNAME).toString();
			type = ddlMapper.mapDataType(rt.getValue(i,RTIDX_COLTYPE).toString());
			length = (BigDecimal)rt.getValue(i,RTIDX_COLLENG);
			precision = (BigDecimal)rt.getValue(i,RTIDX_COLLENG);
			scale = (BigDecimal)rt.getValue(i,RTIDX_COLLENG);
			nullable = parseBooleanFromString(rt.getValue(i,RTIDX_COLNULL).toString());
			defaultValue = parseColumnDefaultValue(rt.getValue(i,RTIDX_COLDEFL));
			try
			{
				columns[i] = new SqlField(name,type,length == null ? 0 : length.intValue(),
						precision != null ? precision.intValue() : null,
						scale != null ? scale.intValue() : null,!nullable,false,defaultValue);
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			columns[i].setOwner(table);
		}
		return columns;
	}
	
	
	/**
	 * @param table
	 * @return
	 * @throws SQLEngineException
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor#loadIndices(com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity)
	 */
	@Override
	public SqlIndex[] loadIndices(final SqlTableIdentity table) throws SQLEngineException
	{
		final String selectInformationSchemaColumns = createSelect_INFORMATION_SCHEMA_INDICES(table);
		final ResultTable rt = new ResultTable(getDbmsAdaptor().getDatabaseGateway().execute(
				SqlExecutor.query,selectInformationSchemaColumns));
		final Oracle12cDDLMapper ddlMapper = this.getDbmsAdaptor().getDdlMapper();
		
		final int rowCount = rt.getRowCount();
		final SqlIndex[] indices = new SqlIndex[rowCount];
		
		INDEXTYPE type = null;
		for(int i = 0; i < rowCount; i++)
		{
			final String[] columnList = rt.getValue(i,RTIDX_IDXCOLS).toString().split(",");
			if(rt.getValue(i,RTIDX_IDXPRIM) != null)
			{
				indices[i] = new SqlPrimaryKey((Object[])columnList);
				indices[i].setName(rt.getValue(i,RTIDX_IDXNAME).toString());
				indices[i].setOwner(table);
			}
			else
			{
				type = ddlMapper
						.mapIndexType(rt.getValue(i,RTIDX_IDXTYPE).toString().toUpperCase());
				indices[i] = new SqlIndex(rt.getValue(i,RTIDX_IDXNAME).toString(),table,type,
						(Object[])columnList);
			}
		}
		return indices;
	}
	
	
	/**
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor#getRetrospectionCodeGenerationNote()
	 */
	@Override
	public String getRetrospectionCodeGenerationNote()
	{
		return "SQLEngine Retrospection for Oracle DBMS.";
	}
	
	
	/*
	 * SELECT COL.column_name, COL.data_type, COL.data_length,
	 * COL.data_precision, COL.data_scale, COL.nullable, IC.Uniqueness
	 * 
	 * FROM USER_TAB_COLUMNS COL
	 * 
	 * LEFT JOIN ( SELECT MIN(IC.column_name) AS columnName, IND.index_type,
	 * IND.UNIQUENESS
	 * 
	 * FROM USER_INDEXES IND
	 * 
	 * INNER JOIN USER_IND_COLUMNS IC ON IC.table_name = IND.table_name AND
	 * IC.index_name = IND.index_name
	 * 
	 * WHERE IC.table_name = ? AND UNIQUENESS = 'UNIQUE'
	 * 
	 * GROUP BY IND.index_name, IND.index_type, IND.UNIQUENESS
	 * 
	 * HAVING COUNT(IC.column_name) = 1 ) IC ON IC.columnName = COL.column_name
	 * 
	 * WHERE COL.table_name = ?
	 */
	/**
	 * @param table
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor#createSelect_INFORMATION_SCHEMA_COLUMNS(com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity)
	 */
	@Override
	public String createSelect_INFORMATION_SCHEMA_COLUMNS(final SqlTableIdentity table)
	{
		final String escapedTableName = apo + table.sql().name + apo;
		
		// (10.02.2010)TODO remove uniqueness
		final String query = SELECT + NEW_LINE + __ + COLd + "column_name" + _AS_ + RTCOL_COLNAME
				+ comma + NEW_LINE + __ + COLd + "data_type" + _AS_ + RTCOL_COLTYPE + comma
				+ NEW_LINE + __ + COLd + "data_length" + _AS_ + RTCOL_COLLENG + comma + NEW_LINE
				+ __ + COLd + "data_precision" + _AS_ + RTCOL_COLPREC + comma + NEW_LINE + __
				+ COLd + "data_scale" + _AS_ + RTCOL_COLSCAL + comma + NEW_LINE + __ + COLd
				+ "nullable" + _AS_ + RTCOL_COLNULL + comma + NEW_LINE + __ + ICd + "uniqueness"
				+ _AS_ + RTCOL_COLUNIQ + comma + NEW_LINE + __ + COLd + "data_default" + _AS_
				+ RTCOL_COLDEFL + NEW_LINE + NEW_LINE + FROM + _ + TABLE_user_tab_columns + _ + COL
				+ NEW_LINE + NEW_LINE + LEFT_JOIN + _ + par + NEW_LINE + TAB + SELECT + NEW_LINE
				+ TAB + __ + MIN + par + ICd + "column_name" + rap + _AS_ + "columnName" + comma
				+ NEW_LINE + TAB + __ + INDd + "index_type" + comma + NEW_LINE + TAB + __ + INDd
				+ "uniqueness" + NEW_LINE + TAB + NEW_LINE + TAB + FROM + _ + TABLE_user_indexes
				+ _ + IND + NEW_LINE + TAB + NEW_LINE + TAB + INNER_JOIN + _
				+ TABLE_user_ind_columns + _ + IC + _ + ON + _ + ICd + "table_name" + _eq_ + INDd
				+ "table_name" + NEW_LINE + TAB + __ + AND + _ + ICd + "index_name" + _eq_ + INDd
				+ "index_name" + NEW_LINE + TAB + NEW_LINE + TAB + WHERE + _ + ICd + "table_name"
				+ _eq_ + escapedTableName + NEW_LINE + TAB + __ + AND + _ + INDd + "uniqueness"
				+ _eq_ + "'UNIQUE'" + NEW_LINE + TAB + NEW_LINE + TAB + GROUP_BY + _ + INDd
				+ "index_name" + comma_ + INDd + "index_type" + comma_ + INDd + "uniqueness"
				+ NEW_LINE + TAB + NEW_LINE + TAB + HAVING + _ + COUNT + par + ICd + "column_name"
				+ rap + _eq_ + 1 + NEW_LINE + rap + _ + IC + _ + ON + _ + ICd + "columnName" + _eq_
				+ COLd + "column_name" + NEW_LINE + NEW_LINE + WHERE + _ + COLd + "table_name"
				+ _eq_ + escapedTableName;
		return query;
	}
	
	
	/**
	 * @param table
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor#createSelect_INFORMATION_SCHEMA_INDICES(com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity)
	 */
	@Override
	public String createSelect_INFORMATION_SCHEMA_INDICES(final SqlTableIdentity table)
	{
		final String escapedTableName = apo + table.sql().name + apo;
		final String query = SELECT + NEW_LINE + __ + INDd + "index_name" + _AS_ + RTCOL_IDXNAME
				+ comma + NEW_LINE + __ + INDd + "index_type" + _AS_ + RTCOL_IDXTYPE + comma
				+ NEW_LINE + __ + INDd + "uniqueness" + _AS_ + RTCOL_IDXUNIQ + comma + NEW_LINE
				+ __ + CONd + "constraint_type" + _AS_ + RTCOL_IDXPRIM + comma + NEW_LINE + __
				+ "wm_concat" + par + ICd + "column_name" + rap + _AS_ + RTCOL_IDXCOLS + NEW_LINE
				+ NEW_LINE + FROM + _ + TABLE_user_indexes + _ + IND + NEW_LINE + NEW_LINE
				+ INNER_JOIN + _ + TABLE_user_ind_columns + _ + IC + _ + ON + _ + ICd
				+ "table_name" + _eq_ + INDd + "table_name" + NEW_LINE + __ + AND + _ + ICd
				+ "index_name" + _eq_ + INDd + "index_name" + NEW_LINE + NEW_LINE + LEFT_JOIN + _
				+ TABLE_user_constraints + _ + CON + _ + ON + _ + CONd + "constraint_name" + _eq_
				+ INDd + "index_name" + NEW_LINE + __ + AND + _ + CONd + "table_name" + _eq_ + INDd
				+ "table_name" + NEW_LINE + __ + AND + _ + CONd + "constraint_type" + _eq_ + "'P'"
				+ NEW_LINE + NEW_LINE + WHERE + _ + INDd + "table_name" + _eq_ + escapedTableName
				+ NEW_LINE + NEW_LINE + GROUP_BY + _ + INDd + "index_name" + comma_ + INDd
				+ "index_type" + comma_ + INDd + "uniqueness" + comma_ + CONd + "constraint_type";
		return query;
	}
	
	
	/**
	 * @param schemaInclusionPatterns
	 * @param schemaExcluionPatterns
	 * @param tableIncluionPatterns
	 * @param tableExcluionPatterns
	 * @param additionalWHERECondition
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor#createSelect_INFORMATION_SCHEMA_TABLES(java.lang.String[],
	 *      java.lang.String[], java.lang.String[], java.lang.String[],
	 *      java.lang.String)
	 */
	@Override
	public String createSelect_INFORMATION_SCHEMA_TABLES(final String[] schemaInclusionPatterns,
			final String[] schemaExcluionPatterns, final String[] tableIncluionPatterns,
			final String[] tableExcluionPatterns, final String additionalWHERECondition)
	{
		/*
		 * SELECT sys_context('userenv', 'current_schema') AS SqlSchemaName,
		 * table_name AS SqlTableName FROM user_tables;
		 */
		
		final StringBuilder sb = new StringBuilder(1024);
		sb.append(QueryLoadTables_WHERE_Table_LIKE_);
		appendIncludeExcludeConditions(
			sb,
			null,
		    null, 
		    tableIncluionPatterns,
		    tableExcluionPatterns,
		    null, 
		    COLUMN_table_name
		    );
		if(additionalWHERECondition != null && additionalWHERECondition.length() > 0)
		{
			sb.append(NEW_LINE).append(AND).append(_).append(additionalWHERECondition);
		}
		
		return sb.toString();
	}
	
	
	/**
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor#getSystemTable_COLUMNS()
	 */
	@Override
	public SqlTableIdentity getSystemTable_COLUMNS()
	{
		return SYSTEMTABLE_COLUMNS;
	}
	
	
	/**
	 * Gets the system table_ indices.
	 * 
	 * @return the system table_ indices
	 */
	public SqlTableIdentity getSystemTable_INDICES()
	{
		return SYSTEMTABLE_INDICES;
	}
	
	
	/**
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor#getSystemTable_TABLES()
	 */
	@Override
	public SqlTableIdentity getSystemTable_TABLES()
	{
		return SYSTEMTABLE_TABLES;
	}
	
}
