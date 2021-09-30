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


import static com.xdev.jadoth.sqlengine.SQL.LANG.FROM;
import static com.xdev.jadoth.sqlengine.SQL.LANG.SELECT;
import static com.xdev.jadoth.sqlengine.SQL.LANG.UNION;
import static com.xdev.jadoth.sqlengine.SQL.LANG.UNION_ALL;
import static com.xdev.jadoth.sqlengine.SQL.LANG.WHERE;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation._;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.dot;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.lte;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.n;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.par;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.rap;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.star;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.indent;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.isOmitAlias;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.isSingleLine;

import com.xdev.jadoth.sqlengine.SELECT;
import com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor;
import com.xdev.jadoth.sqlengine.dbms.DbmsConfiguration;
import com.xdev.jadoth.sqlengine.dbms.DbmsSyntax;
import com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler;
import com.xdev.jadoth.sqlengine.exceptions.SQLEngineInvalidIdentifier;
import com.xdev.jadoth.sqlengine.internal.QueryPart;
import com.xdev.jadoth.sqlengine.internal.SqlColumn;
import com.xdev.jadoth.sqlengine.internal.SqlExpression;
import com.xdev.jadoth.sqlengine.internal.SqlIdentifier;
import com.xdev.jadoth.sqlengine.internal.SqlxAggregateCOLLECT_asString;
import com.xdev.jadoth.sqlengine.internal.interfaces.SelectItem;
import com.xdev.jadoth.sqlengine.internal.interfaces.TableExpression;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;
import com.xdev.jadoth.sqlengine.types.Query;


/**
 * The Class Oracle11gDMLAssembler.
 */
public class Oracle12cDMLAssembler extends StandardDMLAssembler<Oracle12cDbms>
{
	// /////////////////////////////////////////////////////////////////////////
	// constants //
	// ///////////////////
	
	// lol oracle... Why "W", why "M" and why "CONCAT"?
	/** The Constant FUNCTION_AGG_WM_CONCAT. */
	public static final String		FUNCTION_AGG_WM_CONCAT	= "WM_CONCAT";
	
	/** The Constant SELECT_star_FROM_. */
	protected static final String	SELECT_star_FROM_		= SELECT + _ + star + _ + FROM + _
																	+ par + n;
	
	/** The Constant WHERE_ROWNUM_lte_. */
	protected static final String	WHERE_ROWNUM_lte_		= rap + n + _ + WHERE + _ + "ROWNUM"
																	+ _ + lte + _;
	
	protected static final char		DELIM					= '"';
	
	
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	/**
	 * Instantiates a new oracle11g dml assembler.
	 * 
	 * @param oracle11gDbms
	 *            the oracle11g dbms
	 */
	public Oracle12cDMLAssembler(final Oracle12cDbms oracle11gDbms)
	{
		super(oracle11gDbms);
	}
	
	
	@Override
	public StringBuilder assembleColumn(SqlColumn column, StringBuilder sb, int indentLevel,
			int flags)
	{
		final TableExpression owner = column.getOwner();
		
		flags |= QueryPart.bitDelimitColumnIdentifiers(this.getDbmsAdaptor().getConfiguration()
				.isDelimitColumnIdentifiers());
		final String columnName = column.getColumnName();
		boolean delim = false;
		if(columnName != null && !"*".equals(columnName))
		{
			int upper = 0, lower = 0;
			for(char ch : columnName.toCharArray())
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
					SqlIdentifier.validateIdentifierString(columnName);
				}
				catch(SQLEngineInvalidIdentifier e)
				{
					delim = true;
				}
			}
		}
		
		if(owner != null && !QueryPart.isUnqualified(flags))
		{
			this.assembleColumnQualifier(column,sb,flags);
		}
		
		if(delim)
		{
			sb.append(DELIM);
		}
		
		QueryPart.assembleObject(column.getExpressionObject(),this,sb,indentLevel,flags);
		
		if(delim)
		{
			sb.append(DELIM);
		}
		
		return sb;
	}
	
	
	@Override
	public StringBuilder assembleTableIdentifier(SqlTableIdentity table, StringBuilder sb,
			int indentLevel, int flags)
	{
		final DbmsAdaptor<?> dbms = this.getDbmsAdaptor();
		final DbmsSyntax<?> syntax = dbms.getSyntax();
		final DbmsConfiguration<?> config = dbms.getConfiguration();
		
		final SqlTableIdentity.Sql sql = table.sql();
		final String schema = sql.schema;
		final String name = sql.name;
		boolean delim = false;
		if(schema != null)
		{
			try
			{
				SqlIdentifier.validateIdentifierString(schema);
			}
			catch(SQLEngineInvalidIdentifier e)
			{
				delim = true;
			}
		}
		if(name != null)
		{
			try
			{
				SqlIdentifier.validateIdentifierString(name);
			}
			catch(SQLEngineInvalidIdentifier e)
			{
				delim = true;
			}
		}
		
		if(delim)
		{
			sb.append(DELIM);
		}
		
		if(schema != null)
		{
			sb.append(schema).append(dot);
		}
		sb.append(name);
		
		if(delim)
		{
			sb.append(DELIM);
		}
		
		if(!isOmitAlias(flags))
		{
			final String alias = sql.alias;
			if(alias != null && alias.length() > 0)
			{
				sb.append(_);
				if(config.isDelimitAliases() || config.isAutoEscapeReservedWords()
						&& syntax.isReservedWord(alias))
				{
					sb.append(DELIM).append(alias).append(DELIM);
				}
				else
				{
					sb.append(alias);
				}
			}
		}
		return sb;
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// override methods //
	// ///////////////////
	/**
	 * @param query
	 * @param sb
	 * @param indentLevel
	 * @param flags
	 * @param clauseSeperator
	 * @param newLine
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler#assembleSELECT(com.xdev.jadoth.sqlengine.SELECT,
	 *      java.lang.StringBuilder, int, int, java.lang.String,
	 *      java.lang.String)
	 */
	@Override
	protected StringBuilder assembleSELECT(final SELECT query, final StringBuilder sb,
			final int indentLevel, final int flags, final String clauseSeperator,
			final String newLine)
	{
		indent(sb,indentLevel,isSingleLine(flags)).append(query.keyword());
		this.assembleSelectDISTINCT(query,sb,indentLevel,flags);
		this.assembleSelectItems(query,sb,flags,indentLevel,newLine);
		this.assembleSelectSqlClauses(query,sb,indentLevel,flags,clauseSeperator,newLine);
		this.assembleAppendSELECTs(query,sb,indentLevel,flags,clauseSeperator,newLine);
		return sb;
	}
	
	
	/**
	 * @param query
	 * @param sb
	 * @param indentLevel
	 * @param flags
	 * @param clauseSeperator
	 * @param newLine
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler#assembleAppendSELECTs(com.xdev.jadoth.sqlengine.SELECT,
	 *      java.lang.StringBuilder, int, int, java.lang.String,
	 *      java.lang.String)
	 */
	@Override
	protected StringBuilder assembleAppendSELECTs(final SELECT query, final StringBuilder sb,
			final int indentLevel, final int flags, final String clauseSeperator,
			final String newLine)
	{
		SELECT appendSelect = query.getUnionSelect();
		if(appendSelect != null)
		{
			this.assembleAppendSelect(appendSelect,sb,indentLevel,flags,clauseSeperator,newLine,
					UNION);
			return sb;
		}
		
		appendSelect = query.getUnionAllSelect();
		if(appendSelect != null)
		{
			this.assembleAppendSelect(appendSelect,sb,indentLevel,flags,clauseSeperator,newLine,
					UNION_ALL);
			return sb;
		}
		return sb;
	}
	
	
	/**
	 * @param query
	 * @param sb
	 * @param flags
	 * @param clauseSeperator
	 * @param newLine
	 * @param indentLevel
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler#assembleSelectRowLimit(com.xdev.jadoth.sqlengine.SELECT,
	 *      java.lang.StringBuilder, int, java.lang.String, java.lang.String,
	 *      int)
	 */
	@Override
	protected StringBuilder assembleSelectRowLimit(final SELECT query, final StringBuilder sb,
			final int flags, final String clauseSeperator, final String newLine,
			final int indentLevel)
	{
		/*
		 * (13.11.2009 TM)NOTE: phew, this will get get really ugly...
		 * http://troels.arvin.dk/db/rdbms/#select
		 * 
		 * SELECT * FROM ( [query] with inserted
		 * "ROW_NUMBER() OVER (ORDER BY key ASC) AS rownumber, *" Problem:
		 * Oracle can't handle "... AS rownumber, *" must be solved by some
		 * extra QueryModifier. (damn Oracle)
		 * 
		 * 
		 * Maybe simpler: SELECT * FROM ( query ) WHERE rownum <= X
		 * http://www.administrator.de/index.php?content=77754
		 */
		indent(sb,indentLevel,isSingleLine(flags));
		sb.append(SELECT_star_FROM_);
		
		this.assembleSELECT(query,sb,indentLevel,flags,clauseSeperator,newLine);
		
		indent(sb,indentLevel,isSingleLine(flags));
		sb.append(WHERE_ROWNUM_lte_).append(query.getFetchFirstRowCount());
		
		return sb;
	}
	
	
	/**
	 * @param query
	 * @param sb
	 * @param indentLevel
	 * @param flags
	 * @param clauseSeperator
	 * @param newLine
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler#assembleQuerySubclassContext(net.net.jadoth.sqlengine.interfaces.TableQuery,
	 *      java.lang.StringBuilder, int, int, java.lang.String,
	 *      java.lang.String)
	 */
	@Override
	protected StringBuilder assembleQuerySubclassContext(final Query query, final StringBuilder sb,
			final int indentLevel, final int flags, final String clauseSeperator,
			final String newLine)
	{
		if(query instanceof SELECT && ((SELECT)query).getFetchFirstRowCount() != null)
		{
			return this.assembleSelectRowLimit((SELECT)query,sb,flags,clauseSeperator,newLine,
					indentLevel);
		}
		return super.assembleQuerySubclassContext(query,sb,indentLevel,flags,clauseSeperator,
				newLine);
	}
	
	
	/**
	 * @param selectItem
	 * @param sb
	 * @param indentLevel
	 * @param flags
	 * @see com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler#assembleSelectItem(com.xdev.jadoth.sqlengine.internal.interfaces.SelectItem,
	 *      java.lang.StringBuilder, int, int)
	 */
	@Override
	public void assembleSelectItem(final SelectItem selectItem, final StringBuilder sb,
			final int indentLevel, final int flags)
	{
		if(selectItem instanceof SqlExpression)
		{
			assembleExpression((SqlExpression)selectItem,sb,indentLevel,flags);
		}
		else
		{
			super.assembleSelectItem(selectItem,sb,indentLevel,flags);
		}
	}
	
	
	/**
	 * @param expression
	 * @param sb
	 * @param indentLevel
	 * @param flags
	 * @see com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler#assembleExpression(com.xdev.jadoth.sqlengine.internal.SqlExpression,
	 *      java.lang.StringBuilder, int, int)
	 */
	@Override
	public void assembleExpression(final SqlExpression expression, final StringBuilder sb,
			final int indentLevel, final int flags)
	{
		if(expression instanceof SqlxAggregateCOLLECT_asString)
		{
			sb.append(QueryPart.function(this,FUNCTION_AGG_WM_CONCAT,flags,
					(Object[])((SqlxAggregateCOLLECT_asString)expression).getParameters()));
		}
		else
		{
			super.assembleExpression(expression,sb,indentLevel,flags);
		}
	}
	
}
