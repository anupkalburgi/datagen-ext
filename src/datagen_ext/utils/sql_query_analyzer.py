from sqlglot import parse_one, exp
from sqlglot.optimizer.scope import build_scope
from typing import Dict, List, Set, Optional


class SQLQueryAnalyzer:
    """
    A class to analyze SQL queries and extract table relationships and column information.

    This class provides methods to:
    - Extract table relationships from JOIN clauses
    - Identify columns used in queries
    - Map columns to their respective tables
    - Handle both ON and USING join conditions
    """

    def __init__(self):
        """Initialize the SQL Query Analyzer."""
        pass

    def get_table_from_column(self, column: exp.Column, scope) -> Optional[str]:
        """Get the actual table name from a column reference."""
        if not column.table:
            return None

        source = scope.sources.get(column.table)
        if source and isinstance(source, exp.Table):
            return str(source.this)
        return None

    def clean_table_name(self, table_name: str) -> str:
        """Clean table name by removing alias."""
        # Handle both "AS" and "as" (case-insensitive)
        if " AS " in table_name.upper():
            # Find the position of "AS" (case-insensitive)
            as_pos = table_name.upper().find(" AS ")
            return table_name[:as_pos].strip()
        return table_name

    def analyze_joins(self, scope) -> List[Dict]:
        """Analyze JOIN clauses and extract relationships."""
        joins = scope.expression.args.get("joins", [])
        relationships = []

        # Get the base table name from the FROM clause
        base_table = scope.expression.args.get("from")
        base_table_name = self.clean_table_name(str(base_table.this)) if base_table else None

        for join in joins:
            # Get the join condition
            condition = join.args.get("on")
            using = join.args.get("using")

            # Get the right table (the one being joined)
            right_table = join.this
            right_table_name = self.clean_table_name(str(scope.sources[right_table.alias_or_name].this))

            if condition:
                # Flatten nested AND conditions
                conditions = []
                def flatten_conditions(cond):
                    if isinstance(cond, exp.And):
                        flatten_conditions(cond.left)
                        flatten_conditions(cond.right)
                    else:
                        conditions.append(cond)

                flatten_conditions(condition)

                for cond in conditions:
                    if isinstance(cond, exp.EQ):
                        # Handle ON condition
                        left_key = cond.left
                        right_key = cond.right

                        # Only process if both sides are columns (not literals or other expressions)
                        if isinstance(left_key, exp.Column) and isinstance(right_key, exp.Column):
                            # Get the actual table names from the column references
                            left_table_name = self.get_table_from_column(left_key, scope)
                            right_table_name_from_key = self.get_table_from_column(right_key, scope)

                            # If we couldn't determine the left table from the column, use the right table from the key
                            if not left_table_name and right_table_name_from_key:
                                left_table_name = right_table_name_from_key
                                right_table_name = right_table_name

                            relationships.append({
                                "left": left_table_name,
                                "right": right_table_name,
                                "left_key": left_key.name,
                                "right_key": right_key.name,
                                "type": "on"
                            })
            elif using:
                # Handle USING clause - using is a list of expressions
                for column in using:
                    relationships.append({
                        "left": base_table_name,
                        "right": right_table_name,
                        "left_key": column.name,
                        "right_key": column.name,
                        "type": "using"
                    })

        return relationships

    def get_table_columns(self, scope) -> Dict[str, Set[str]]:
        """Get columns from scope.columns (basic method)."""
        table_columns = {}

        # Get all columns in the query
        for column in scope.columns:
            table_name = column.table
            if table_name:
                # Get the actual table name from the source
                source = scope.sources.get(table_name)
                if source and isinstance(source, exp.Table):
                    actual_table = self.clean_table_name(str(source.this))
                    if actual_table not in table_columns:
                        table_columns[actual_table] = set()
                    table_columns[actual_table].add(column.name)

        # Also get columns from the SELECT clause to catch any missing ones
        for select in scope.expression.selects:
            if isinstance(select, exp.Column):
                table_name = select.table
                if table_name:
                    source = scope.sources.get(table_name)
                    if source and isinstance(source, exp.Table):
                        actual_table = self.clean_table_name(str(source.this))
                        if actual_table not in table_columns:
                            table_columns[actual_table] = set()
                        table_columns[actual_table].add(select.name)

        return table_columns

    def get_all_columns_from_query(self, scope) -> Dict[str, Set[str]]:
        """Get all columns referenced in the query including SELECT, WHERE, GROUP BY, etc."""
        table_columns = {}

        # First, try to get columns from scope.columns (this is the most reliable)
        for column in scope.columns:
            table_name = column.table
            if table_name:
                source = scope.sources.get(table_name)
                if source and isinstance(source, exp.Table):
                    actual_table = self.clean_table_name(str(source.this))
                    if actual_table not in table_columns:
                        table_columns[actual_table] = set()
                    table_columns[actual_table].add(column.name)

        # Then walk through all expressions to find any missing columns
        for node in scope.expression.walk():
            if isinstance(node, exp.Column):
                table_name = node.table
                column_name = node.name

                if table_name:
                    source = scope.sources.get(table_name)
                    if source and isinstance(source, exp.Table):
                        actual_table = self.clean_table_name(str(source.this))
                        if actual_table not in table_columns:
                            table_columns[actual_table] = set()
                        table_columns[actual_table].add(node.name)
                else:
                    # Try to infer table from column name pattern and scope sources
                    inferred_table = self.infer_table_from_column_name(column_name, scope)
                    if inferred_table:
                        if inferred_table not in table_columns:
                            table_columns[inferred_table] = set()
                        table_columns[inferred_table].add(node.name)
                    else:
                        # Last resort: try to find the table by looking at the context
                        # This is useful for columns that appear in SELECT but have no table reference
                        context_table = self.find_table_from_context(node, scope)
                        if context_table:
                            if context_table not in table_columns:
                                table_columns[context_table] = set()
                            table_columns[context_table].add(node.name)

        # Handle USING clauses - add USING columns to both tables involved
        joins = scope.expression.args.get("joins", [])
        base_table = scope.expression.args.get("from")
        base_table_name = self.clean_table_name(str(base_table.this)) if base_table else None

        for join in joins:
            using = join.args.get("using")
            if using:
                # Get the right table (the one being joined)
                right_table = join.this
                right_table_name = self.clean_table_name(str(scope.sources[right_table.alias_or_name].this))

                # Add USING columns to both tables
                for column in using:
                    column_name = column.name

                    # Add to base table
                    if base_table_name:
                        if base_table_name not in table_columns:
                            table_columns[base_table_name] = set()
                        table_columns[base_table_name].add(column_name)

                    # Add to right table
                    if right_table_name not in table_columns:
                        table_columns[right_table_name] = set()
                    table_columns[right_table_name].add(column_name)

        return table_columns

    def infer_table_from_column_name(self, column_name: str, scope) -> Optional[str]:
        """Try to infer the table name from column name patterns and scope sources."""
        # Get all available table aliases and their actual table names
        table_mapping = {}
        for alias, source in scope.sources.items():
            if isinstance(source, exp.Table):
                actual_table = str(source.this)
                table_mapping[alias] = actual_table

        # Try to match column name with table aliases
        for alias, actual_table in table_mapping.items():
            # Check if column name starts with the table alias followed by underscore
            if column_name.startswith(f"{alias}_"):
                return actual_table

        # If no direct alias match, try to find the most likely table
        # by looking at the column name and available tables
        for alias, actual_table in table_mapping.items():
            # Check if the column name contains the table name or alias
            # But be more specific to avoid false matches
            if (alias.lower() in column_name.lower() or
                    actual_table.lower() in column_name.lower()):
                # Additional check: make sure it's not just a partial match
                # For example, "unknown_column" shouldn't match "u" from alias "u"
                if (alias.lower() == column_name.lower() or
                        actual_table.lower() == column_name.lower() or
                        column_name.lower().startswith(alias.lower() + "_") or
                        column_name.lower().startswith(actual_table.lower() + "_")):
                    return actual_table

        return None

    def find_table_from_context(self, column_node: exp.Column, scope) -> Optional[str]:
        """Try to find the table for a column by looking at the query context."""
        # Get all available tables
        available_tables = {}
        for alias, source in scope.sources.items():
            if isinstance(source, exp.Table):
                actual_table = str(source.this)
                available_tables[actual_table] = alias

        # If there's only one table, it's likely the source
        if len(available_tables) == 1:
            return list(available_tables.keys())[0]

        # Try to find the most likely table based on the column name
        column_name = column_node.name.lower()

        # Check if any table name is contained in the column name
        for table_name in available_tables.keys():
            if table_name.lower() in column_name:
                return table_name

        # Check if any table alias is contained in the column name
        for table_name, alias in available_tables.items():
            if alias.lower() in column_name:
                return table_name

        return None

    def analyze_query(self, query: str) -> Dict:
        """
        Analyze a SQL query and return table relationships and column information.

        Args:
            query (str): The SQL query to analyze

        Returns:
            Dict: A dictionary containing:
                - 'tables': Dict mapping table names to sets of column names
                - 'relationships': List of join relationships between tables
        """
        expression = parse_one(query)
        scope = build_scope(expression)

        # Get table relationships
        relationships = self.analyze_joins(scope)

        # Get column information using the comprehensive method
        table_columns = self.get_all_columns_from_query(scope)

        return {
            "tables": table_columns,
            "relationships": relationships
        }
