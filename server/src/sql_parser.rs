
use pest::{iterators::{Pair, Pairs}, Parser};
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "grammar.pest"]
pub struct QueryParser;


#[derive(Debug)]
pub struct SqlQuery {
    pub with_statements: Vec<SqlWith>,
    pub select_statmeent: SqlSelect,
    pub raw: String,
}

#[derive(Debug)]
pub struct SqlWith {
    pub name: String,
    pub raw: String,
}

#[derive(Debug)]
pub struct SqlSelect {
    pub raw: String,
}

pub fn parse_sql(text: &str) -> Result<SqlQuery, pest::error::Error<Rule>> {
    let query_ast = QueryParser::parse(Rule::query, text)?;

    let mut with_blocks: Vec<SqlWith> = Vec::new();

    for stmt in query_ast.clone().next().unwrap().into_inner() { // never fails
        if stmt.as_rule() == Rule::with_block {
            with_blocks.push(parse_with_block(&stmt));
        }
        if stmt.as_rule() == Rule::select_query {
            return Ok(SqlQuery {
                with_statements: with_blocks,
                select_statmeent: parse_select_query(&stmt),
                raw: text.to_string()
            })
        }
    }
    unreachable!()
}

fn parse_with_block(pair: &Pair<'_, Rule>) -> SqlWith {
    let text = pair.as_str().to_string();
    for p in pair.clone().into_inner() {
        if p.as_rule() == Rule::identifier {
            return SqlWith {name: p.as_str().to_string(), raw: text};
        }
    }
    unreachable!()
}

fn parse_select_query(pair: &Pair<'_, Rule>) -> SqlSelect {
    SqlSelect { raw: pair.as_str().to_string() }
}


#[cfg(test)]
mod tests {
    use super::*;
    use regex::Regex;

    #[test]
    fn test_parse_sql() {
        let text = r#"
        with users as (
            select name, age from mysql_users
        ),
        select `name`, age
          from users
          where (name in ('John')) AND age > 25
        "#;
        let result = parse_sql(text);

        assert!(result.is_ok());
        let sql_obj = result.unwrap();

        assert_eq!(sql_obj.with_statements.len(), 1);
        assert_eq!(sql_obj.with_statements[0].name, String::from("users"));

        let re = Regex::new(r"(\n|\s)+").unwrap();
        assert_eq!(
            re.replace_all(&sql_obj.with_statements[0].raw, " ").trim(),
            "with users as ( select name, age from mysql_users ),".to_string()
        );
        assert_eq!(
            re.replace_all(&sql_obj.select_statmeent.raw, " ").trim(),
            "select `name`, age from users where (name in ('John')) AND age > 25".to_string()
        );
    }

    #[test]
    fn test_simple_select() {
        let text = "select `name`, age from users";

        let successful_parse = QueryParser::parse(Rule::query, text);
        println!("{:?}", successful_parse);


        for node in successful_parse.unwrap().next().unwrap().into_inner() {
            println!("=> {:?}", node.as_rule())
        }
    }

    #[test]
    fn test_select_with_where() {
        let text = r#"
        select `name`, age
        from users
        where (name in ('John')) AND age > 25
        "#;

        let successful_parse = QueryParser::parse(Rule::query, text);
        println!("{:?}", successful_parse);
    }

    #[test]
    fn test_with_block() {
        let text = r#"
        with users as (select name, age from mysql_users),
        select `name`, age
        from users
        where (name in ('John')) AND age > 25
        "#;

        let successful_parse = QueryParser::parse(Rule::query, text);
        println!("{:?}", successful_parse);
    }

    #[test]
    fn test_full_query() {
        let text = r#"
        WITH employees AS (
            SELECT id, name
            FROM users
            WHERE position like 'employee%'
          ),
          
          SELECT employees.name, sum(amount) AS total
          FROM employees JOIN payments
            ON employees.id = payments.user_id
          WHERE employees.active = 1
          GROUP BY employees.name
          HAVING total > 1000
          ORDER BY total
          ;
        "#;

        let successful_parse = QueryParser::parse(Rule::query, text);
        println!("{:?}", successful_parse);
    }
}