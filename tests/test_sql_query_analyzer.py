import pytest
from datagen_ext.utils.sql_query_analyzer import SQLQueryAnalyzer
from sqlglot import parse_one, exp
from sqlglot.optimizer.scope import build_scope


@pytest.fixture
def analyzer():
    """Fixture to provide SQLQueryAnalyzer instance."""
    return SQLQueryAnalyzer()


class TestSQLQueryAnalyzer:
    """Test cases for SQLQueryAnalyzer class."""

    def test_simple_select(self, analyzer):
        """Test simple SELECT statement without joins."""
        query = "SELECT id, name FROM users"
        result = analyzer.analyze_query(query)

        assert 'tables' in result
        assert 'relationships' in result
        assert 'users' in result['tables']
        assert result['tables']['users'] == {'id', 'name'}
        assert result['relationships'] == []

    def test_select_with_alias(self, analyzer):
        """Test SELECT statement with table alias."""
        query = "SELECT u.id, u.name FROM users AS u"
        result = analyzer.analyze_query(query)

        assert 'users' in result['tables']
        assert result['tables']['users'] == {'id', 'name'}

    def test_simple_join_on(self, analyzer):
        """Test simple JOIN with ON condition."""
        query = """
        SELECT u.name, p.title 
        FROM users u 
        JOIN posts p ON u.id = p.user_id
        """
        result = analyzer.analyze_query(query)

        assert 'users' in result['tables']
        assert 'posts' in result['tables']
        assert result['tables']['users'] == {'name', 'id'}
        assert result['tables']['posts'] == {'title', 'user_id'}

        assert len(result['relationships']) == 1
        rel = result['relationships'][0]
        assert rel['left'] == 'users'
        assert rel['right'] == 'posts'
        assert rel['left_key'] == 'id'
        assert rel['right_key'] == 'user_id'
        assert rel['type'] == 'on'

    def test_join_with_using(self, analyzer):
        """Test JOIN with USING clause."""
        query = """
        SELECT a.id, b.salary 
        FROM employees a 
        JOIN salaries b USING (id)
        """
        result = analyzer.analyze_query(query)

        assert 'employees' in result['tables']
        assert 'salaries' in result['tables']
        assert result['tables']['employees'] == {'id'}
        assert result['tables']['salaries'] == {'salary', 'id'}

        assert len(result['relationships']) == 1
        rel = result['relationships'][0]
        assert rel['left'] == 'employees'
        assert rel['right'] == 'salaries'
        assert rel['left_key'] == 'id'
        assert rel['right_key'] == 'id'
        assert rel['type'] == 'using'

    def test_multiple_joins(self, analyzer):
        """Test query with multiple JOINs."""
        query = """
        SELECT u.name, p.title, c.content
        FROM users u 
        JOIN posts p ON u.id = p.user_id
        JOIN comments c ON p.id = c.post_id
        """
        result = analyzer.analyze_query(query)

        assert 'users' in result['tables']
        assert 'posts' in result['tables']
        assert 'comments' in result['tables']

        assert len(result['relationships']) == 2

    def test_compound_join_conditions(self, analyzer):
        """Test JOIN with compound conditions (AND)."""
        query = """
        SELECT o.order_id, c.customer_name
        FROM orders o 
        JOIN customers c ON o.customer_id = c.id AND o.status = 'active'
        """
        result = analyzer.analyze_query(query)

        assert 'orders' in result['tables']
        assert 'customers' in result['tables']
        assert len(result['relationships']) == 1 # Note: o.status = 'active' is a filter not a relation

    def test_warehouse_columns(self, analyzer):
        """Test warehouse columns with w_ prefix."""
        query = """
        SELECT w_warehouse_name, w_warehouse_sk
        FROM warehouse w
        """
        result = analyzer.analyze_query(query)

        assert 'warehouse' in result['tables']
        assert 'w_warehouse_name' in result['tables']['warehouse']
        assert 'w_warehouse_sk' in result['tables']['warehouse']

    def test_complex_query(self, analyzer):
        """Test complex query with multiple joins and conditions."""
        query = """
        SELECT
            i_item_desc,
            w_warehouse_name,
            d1.d_week_seq,
            SUM(CASE WHEN p_promo_sk IS NOT NULL THEN 1 ELSE 0 END) AS promo_count
        FROM catalog_sales cs
        JOIN inventory inv ON cs.cs_item_sk = inv.inv_item_sk
        JOIN warehouse w ON cs.cs_warehouse_sk = w.w_warehouse_sk
        JOIN item i ON cs.cs_item_sk = i.i_item_sk
        JOIN date_dim d1 ON cs.cs_sold_date_sk = d1.d_date_sk
        LEFT OUTER JOIN promotion p ON cs.cs_promo_sk = p.p_promo_sk
        WHERE d1.d_year = 1999
        GROUP BY i_item_desc, w_warehouse_name, d1.d_week_seq
        """
        result = analyzer.analyze_query(query)

        # Check that all tables are captured
        expected_tables = {'catalog_sales', 'inventory', 'warehouse', 'item', 'date_dim', 'promotion'}
        assert set(result['tables'].keys()) == expected_tables

        # Check that we have multiple relationships
        assert len(result['relationships']) > 3

        # Check that warehouse columns are captured
        assert 'w_warehouse_name' in result['tables']['warehouse']

    def test_columns_in_where_clause(self, analyzer):
        """Test that columns in WHERE clause are captured."""
        query = """
        SELECT u.name, p.title
        FROM users u
        JOIN posts p ON u.id = p.user_id
        WHERE u.active = true AND p.status = 'published'
        """
        result = analyzer.analyze_query(query)

        assert 'active' in result['tables']['users']
        assert 'status' in result['tables']['posts']

    def test_columns_in_group_by(self, analyzer):
        """Test that columns in GROUP BY are captured."""
        query = """
        SELECT u.name, COUNT(p.id) as post_count
        FROM users u
        JOIN posts p ON u.id = p.user_id
        GROUP BY u.name, u.id
        """
        result = analyzer.analyze_query(query)

        assert 'name' in result['tables']['users']
        assert 'id' in result['tables']['users']

    def test_columns_in_order_by(self, analyzer):
        """Test that columns in ORDER BY are captured."""
        query = """
        SELECT u.name, p.title
        FROM users u
        JOIN posts p ON u.id = p.user_id
        ORDER BY u.name, p.created_at
        """
        result = analyzer.analyze_query(query)

        assert 'name' in result['tables']['users']
        assert 'created_at' in result['tables']['posts']

    def test_invalid_sql(self, analyzer):
        """Test invalid SQL."""
        query = "SELECT * FROM"
        with pytest.raises(Exception):
            analyzer.analyze_query(query)

    def test_clean_table_name(self, analyzer):
        """Test clean_table_name method."""
        assert analyzer.clean_table_name("users AS u") == "users"
        assert analyzer.clean_table_name("users") == "users"
        assert analyzer.clean_table_name("users as u") == "users"

    def test_get_table_from_column(self, analyzer):
        """Test get_table_from_column method."""
        query = "SELECT u.id FROM users u"
        expression = parse_one(query)
        scope = build_scope(expression)

        # Test with qualified column
        column = exp.Column(this="id", table="u")
        result = analyzer.get_table_from_column(column, scope)
        assert result == "users"

        # Test with unqualified column
        column = exp.Column(this="id")
        result = analyzer.get_table_from_column(column, scope)
        assert result is None

    def test_find_table_from_context_single_table(self, analyzer):
        """Test find_table_from_context with single table."""
        query = "SELECT name FROM users"
        expression = parse_one(query)
        scope = build_scope(expression)

        column = exp.Column(this="name")
        result = analyzer.find_table_from_context(column, scope)
        assert result == "users"

    def test_infer_table_from_column_name(self, analyzer):
        """Test infer_table_from_column_name method."""
        query = "SELECT u_name, p_title FROM users u JOIN posts p ON u.id = p.user_id"
        expression = parse_one(query)
        scope = build_scope(expression)

        # Test with prefix matching
        result = analyzer.infer_table_from_column_name("u_name", scope)
        assert result == "users"

        # Test with no match
        result = analyzer.infer_table_from_column_name("unknown_column", scope)
        assert result is None

    def test_analyze_joins_no_joins(self, analyzer):
        """Test analyze_joins with no joins."""
        query = "SELECT * FROM users"
        expression = parse_one(query)
        scope = build_scope(expression)

        result = analyzer.analyze_joins(scope)
        assert result == []

    def test_analyze_joins_multiple_conditions(self, analyzer):
        """Test analyze_joins with multiple AND conditions."""
        query = """
        SELECT * 
        FROM orders o 
        JOIN customers c ON o.customer_id = c.id AND o.status = 'active' AND c.active = true
        """
        expression = parse_one(query)
        scope = build_scope(expression)

        result = analyzer.analyze_joins(scope)
        assert len(result) == 1  # Three AND conditions

    def test_get_all_columns_from_query_comprehensive(self, analyzer):
        """Test comprehensive column extraction."""
        query = """
        SELECT u.name, p.title, COUNT(c.id) as comment_count
        FROM users u
        JOIN posts p ON u.id = p.user_id
        LEFT JOIN comments c ON p.id = c.post_id
        WHERE u.active = true AND p.status = 'published'
        GROUP BY u.name, p.title
        HAVING COUNT(c.id) > 0
        ORDER BY u.name, p.created_at
        """
        expression = parse_one(query)
        scope = build_scope(expression)

        result = analyzer.get_all_columns_from_query(scope)

        assert 'users' in result
        assert 'posts' in result
        assert 'comments' in result

        # Check that columns from different clauses are captured
        assert 'active' in result['users']
        assert 'status' in result['posts']
        assert 'created_at' in result['posts']
        assert 'id' in result['comments']