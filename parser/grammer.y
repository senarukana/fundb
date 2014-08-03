%{

package parser

import (
    "github.com/senarukana/fundb/protocol"
)

var ParsedQuery *Query

type Token struct {
    Pos    int
    Src    string
}

func (t Token) String() string {
    return t.Src
}

%}

%union {
    sql         *Query
    ident       *Ident
    literal     *Literal
    insert_sql  *InsertQuery
    column_list *ColumnFields
    value_list  *ValueList
    value_items *ValueItems
    tok         Token
} 



%token <tok> LP RP DOT COMMA NULLX 
%token <tok> SELECT UPDATE DELETE INSERT
%token <tok> INTO VALUES WHERE FROM
%token <tok> IDENT STRING DOUBLE INT BOOL
%token <tok> GREATER GREATEREQ SMALLER SMALLEREQ EQUAL

%type <sql> sql manipulative_statement
%type <insert_sql> insert_statement
%type <ident> table column
%type <literal> insert_atom literal
%type <column_list> opt_column_commalist column_commalist
%type <value_list> values_list
%type <value_items> value_items insert_atom_commalist
%type <column_list> opt_column_commalist column_commalist


%start sql

%%
sql: manipulative_statement {
        ParsedQuery = $1
    }
    

manipulative_statement:
        insert_statement {
            $$ = &Query{
                kind : QUERY_INSERT,
                queryAST : $1,
            }
        }

insert_statement:
        INSERT INTO table opt_column_commalist VALUES values_list {
            $$ = &InsertQuery{$3, $4, $6}
        }

opt_column_commalist:
        /* empty */ {
            $$ = &ColumnFields{}
        }
    |   LP column_commalist RP {
            $$ = $2
        }
    ;

column_commalist:
        column {
            $$ = NewColumnField($1)
        }
    |   column_commalist COMMA column {
            $$ = ColumnFieldsAppend($1, $3)
        }
    ;

values_list:
        value_items {
            $$ = NewValueList($1)
        }
    |   values_list COMMA value_items {
            $$ = ValueListAppend($1, $3)
        }

value_items:
        LP insert_atom_commalist RP {
            $$ = $2
        }

insert_atom_commalist:
        insert_atom {
            $$ = NewValueItem($1)
        }
    |   insert_atom_commalist COMMA insert_atom {
            $$ = ValueItemAppend($1, $3)
        }

insert_atom:
        literal {
            $$ = $1
        }
    |   NULLX {
            $$ = &Literal{$1.Pos, protocol.NULL, ""}
        }
    ;

column:     
        IDENT {
            $$ = &Ident{$1.Pos, $1.Src}
        }

table:
        IDENT {
            $$ = &Ident{$1.Pos, $1.Src}
        }

literal:
        STRING {
            $$ = &Literal{$1.Pos, protocol.STRING, $1.Src}
        }
    |   INT {
            $$ = &Literal{$1.Pos, protocol.INT, $1.Src}
        }
    |   DOUBLE {
            $$ = &Literal{$1.Pos, protocol.DOUBLE, $1.Src}
        }
    |   BOOL {
            $$ = &Literal{$1.Pos, protocol.BOOL, $1.Src}
        }

%%