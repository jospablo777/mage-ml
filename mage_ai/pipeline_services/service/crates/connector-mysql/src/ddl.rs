//! The statements of the binary log that change what a stream reads without a row change:
//! TRUNCATE, DROP and RENAME of tables. The stream does not replicate them, and says so when
//! one names a table it reads (ADR-0048).

/// A table, by database and name.
pub(crate) type Name = (String, String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Ddl {
    Truncate(Name),
    Drop(Name),
    DropDatabase(String),
    /// From, to.
    Rename(Name, Name),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Word(String),
    /// A `quoted` name.
    Quoted(String),
    Punct(char),
}

fn tokens(statement: &str) -> Vec<Token> {
    let chars: Vec<char> = statement.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '/' && chars.get(i + 1) == Some(&'*') {
            i += 2;
            while i < chars.len() && !(chars[i] == '*' && chars.get(i + 1) == Some(&'/')) {
                i += 1;
            }
            i += 2;
        } else if c == '#' || (c == '-' && chars.get(i + 1) == Some(&'-')) {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if c == '`' {
            let mut name = String::new();
            i += 1;
            while i < chars.len() {
                if chars[i] == '`' {
                    if chars.get(i + 1) == Some(&'`') {
                        name.push('`');
                        i += 2;
                        continue;
                    }
                    break;
                }
                name.push(chars[i]);
                i += 1;
            }
            i += 1;
            out.push(Token::Quoted(name));
        } else if c == '\'' || c == '"' {
            // A string (a comment, a default): none of its words count.
            i += 1;
            while i < chars.len() {
                if chars[i] == '\\' {
                    i += 2;
                    continue;
                }
                if chars[i] == c {
                    if chars.get(i + 1) == Some(&c) {
                        i += 2;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            i += 1;
            out.push(Token::Punct(c));
        } else if c.is_alphanumeric() || c == '_' || c == '$' {
            let start = i;
            while i < chars.len()
                && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '$')
            {
                i += 1;
            }
            out.push(Token::Word(chars[start..i].iter().collect()));
        } else {
            out.push(Token::Punct(c));
            i += 1;
        }
    }
    out
}

struct Reader<'a> {
    tokens: &'a [Token],
    at: usize,
    database: &'a str,
}

impl Reader<'_> {
    fn keyword(&mut self, word: &str) -> bool {
        match self.tokens.get(self.at) {
            Some(Token::Word(w)) if w.eq_ignore_ascii_case(word) => {
                self.at += 1;
                true
            }
            _ => false,
        }
    }

    fn punct(&mut self, c: char) -> bool {
        if self.tokens.get(self.at) == Some(&Token::Punct(c)) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn identifier(&mut self) -> Option<String> {
        let name = match self.tokens.get(self.at)? {
            Token::Word(w) | Token::Quoted(w) => w.clone(),
            Token::Punct(_) => return None,
        };
        self.at += 1;
        Some(name)
    }

    /// `name` or `database.name`; a bare name is of the statement's default database.
    fn table(&mut self) -> Option<Name> {
        let first = self.identifier()?;
        if self.punct('.') {
            Some((first, self.identifier()?))
        } else {
            Some((self.database.to_string(), first))
        }
    }

    fn tables(&mut self) -> Vec<Name> {
        let mut found = Vec::new();
        while let Some(table) = self.table() {
            found.push(table);
            if !self.punct(',') {
                break;
            }
        }
        found
    }
}

/// What `statement`, run with `database` as its default, does to tables; empty for anything
/// else (row changes come as row events, and new columns with each table map).
pub(crate) fn read(statement: &str, database: &str) -> Vec<Ddl> {
    let tokens = tokens(statement);
    let mut r = Reader {
        tokens: &tokens,
        at: 0,
        database,
    };
    if r.keyword("TRUNCATE") {
        r.keyword("TABLE");
        return r.table().map(Ddl::Truncate).into_iter().collect();
    }
    if r.keyword("DROP") {
        if r.keyword("DATABASE") || r.keyword("SCHEMA") {
            if r.keyword("IF") {
                r.keyword("EXISTS");
            }
            return r.identifier().map(Ddl::DropDatabase).into_iter().collect();
        }
        // A temporary table is the session's own, and never a table a stream reads.
        if r.keyword("TEMPORARY") || !r.keyword("TABLE") {
            return Vec::new();
        }
        if r.keyword("IF") {
            r.keyword("EXISTS");
        }
        return r.tables().into_iter().map(Ddl::Drop).collect();
    }
    if r.keyword("RENAME") {
        if !r.keyword("TABLE") && !r.keyword("TABLES") {
            return Vec::new();
        }
        let mut found = Vec::new();
        while let Some(from) = r.table() {
            if !r.keyword("TO") {
                break;
            }
            let Some(to) = r.table() else { break };
            found.push(Ddl::Rename(from, to));
            if !r.punct(',') {
                break;
            }
        }
        return found;
    }
    if r.keyword("ALTER") {
        // ALTER [ONLINE] [IGNORE] TABLE name ... RENAME [TO | AS] new, among its other changes.
        while !r.keyword("TABLE") {
            if r.identifier().is_none() {
                return Vec::new();
            }
        }
        let Some(from) = r.table() else {
            return Vec::new();
        };
        let mut depth = 0usize;
        while r.at < tokens.len() {
            match &tokens[r.at] {
                Token::Punct('(') => depth += 1,
                Token::Punct(')') => depth = depth.saturating_sub(1),
                Token::Word(w) if depth == 0 && w.eq_ignore_ascii_case("RENAME") => {
                    r.at += 1;
                    // RENAME COLUMN, INDEX and KEY keep the table's name.
                    if ["COLUMN", "INDEX", "KEY"].iter().any(|k| r.keyword(k)) {
                        continue;
                    }
                    if !r.keyword("TO") {
                        r.keyword("AS");
                    }
                    return r
                        .table()
                        .map(|to| Ddl::Rename(from, to))
                        .into_iter()
                        .collect();
                }
                _ => {}
            }
            r.at += 1;
        }
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(db: &str, table: &str) -> Name {
        (db.to_string(), table.to_string())
    }

    #[test]
    fn truncates_and_drops_name_their_tables() {
        assert_eq!(
            read("TRUNCATE TABLE orders", "shop"),
            [Ddl::Truncate(name("shop", "orders"))]
        );
        assert_eq!(
            read("truncate `audit`.`ev``ents`", "shop"),
            [Ddl::Truncate(name("audit", "ev`ents"))]
        );
        // MySQL writes a DROP TABLE with a comment of its own.
        assert_eq!(
            read(
                "DROP TABLE IF EXISTS `orders`,`audit`.`events` /* generated by server */",
                "shop"
            ),
            [
                Ddl::Drop(name("shop", "orders")),
                Ddl::Drop(name("audit", "events"))
            ]
        );
        assert_eq!(
            read("/* app */ DROP DATABASE IF EXISTS shop", "x"),
            [Ddl::DropDatabase("shop".into())]
        );
        assert!(read("DROP TEMPORARY TABLE orders", "shop").is_empty());
        assert!(read("DROP INDEX i ON orders", "shop").is_empty());
    }

    #[test]
    fn renames_name_both_tables() {
        assert_eq!(
            read(
                "RENAME TABLE orders TO old_orders, new_orders TO orders",
                "shop"
            ),
            [
                Ddl::Rename(name("shop", "orders"), name("shop", "old_orders")),
                Ddl::Rename(name("shop", "new_orders"), name("shop", "orders")),
            ]
        );
        assert_eq!(
            read(
                "ALTER TABLE `orders` ADD COLUMN note TEXT, RENAME TO archive.orders",
                "shop"
            ),
            [Ddl::Rename(
                name("shop", "orders"),
                name("archive", "orders")
            )]
        );
        assert_eq!(
            read("alter table orders rename as orders_2", "shop"),
            [Ddl::Rename(
                name("shop", "orders"),
                name("shop", "orders_2")
            )]
        );
        // A column, an index, a default that reads RENAME, and other changes keep the name.
        assert!(read("ALTER TABLE orders RENAME COLUMN a TO b", "shop").is_empty());
        assert!(read("ALTER TABLE orders RENAME INDEX a TO b", "shop").is_empty());
        assert!(read("ALTER TABLE orders ADD COLUMN `rename` INT", "shop").is_empty());
        assert!(read("ALTER TABLE orders COMMENT 'it''s rename to x'", "shop").is_empty());
        assert!(read("ALTER TABLE orders COMMENT \"rename \\\" to x\"", "shop").is_empty());
        assert!(read("ALTER TABLE orders ADD COLUMN note TEXT", "shop").is_empty());
        assert!(read("CREATE TABLE orders (id INT)", "shop").is_empty());
        assert!(read("BEGIN", "shop").is_empty());
    }
}
