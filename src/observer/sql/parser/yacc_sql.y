
%{

#include "sql/parser/parse_defs.h"
#include "sql/parser/yacc_sql.tab.h"
#include "sql/parser/lex.yy.h"
// #include "common/log/log.h" // 包含C++中的头文件

#include<stdio.h>
#include<stdlib.h>
#include<string.h>

typedef struct ParserContext {
  Query * ssql;
  size_t select_length;
  size_t condition_length;
  size_t from_length;
  size_t value_length;
  Value values[MAX_NUM];
  size_t value2_length;
  Value values2[MAX_NUM];
  size_t tuple_length;
  InsertTuple tuples[MAX_NUM];
  Condition conditions[MAX_NUM];
  Selects selection;
  CompOp comp;
	char id[MAX_NUM];
  FuncName func;
} ParserContext;

//获取子串
char *substr(const char *s,int n1,int n2)/*从s中提取下标为n1~n2的字符组成一个新字符串，然后返回这个新串的首地址*/
{
  char *sp = malloc(sizeof(char) * (n2 - n1 + 2));
  int i, j = 0;
  for (i = n1; i <= n2; i++) {
    sp[j++] = s[i];
  }
  sp[j] = 0;
  return sp;
}

void yyerror(yyscan_t scanner, const char *str)
{
  ParserContext *context = (ParserContext *)(yyget_extra(scanner));
  query_reset(context->ssql);
  context->ssql->flag = SCF_ERROR;
  context->condition_length = 0;
  context->from_length = 0;
  context->select_length = 0;
  context->value_length = 0;
  context->tuple_length = 0;
  context->ssql->sstr.insertion.tuple_num = 0;
  printf("parse sql failed. error=%s", str);
}

ParserContext *get_context(yyscan_t scanner)
{
  return (ParserContext *)yyget_extra(scanner);
}

#define CONTEXT get_context(scanner)

%}

%define api.pure full
%lex-param { yyscan_t scanner }
%parse-param { void *scanner }

//标识tokens
%token  SEMICOLON
        CREATE
        DROP
        TABLE
        TABLES
        INDEX
        SELECT
        DESC
        SHOW
        SYNC
        INSERT
        DELETE
        UPDATE
        LBRACE
        RBRACE
        COMMA
        TRX_BEGIN
        TRX_COMMIT
        TRX_ROLLBACK
        INT_T
        STRING_T
        FLOAT_T
        DATE_T
        TEXT_T
        HELP
        EXIT
        DOT //QUOTE
        INTO
        VALUES
        FROM
        WHERE
        AND
        SET
        ON
        LOAD
        DATA
        INFILE
        EQ
        LT
        GT
        LE
        GE
        NE
        LIKE
        NOT //NOT LIKE
        INNER
        JOIN
        NULLABLE
        AGGMAX
	AGGMIN
	AGGCOUNT
	AGGAVG
	AGGSUM
	UNIQUE
	ORDER
	BY
	ASC
	NULL_V

%union {
  struct _Attr *attr;
  struct _Condition *condition1;
  struct _Value *value1;
  char *string;
  int number;
  float floats;
	char *position;
}

%token <number> NUMBER
%token <floats> FLOAT 
%token <string> ID
%token <string> PATH
%token <string> DATE_STR
%token <string> SSS
%token <string> STAR
%token <string> STRING_V
//非终结符

%type <number> type;
%type <condition1> condition;
%type <value1> value;
%type <number> number;

%%

commands:		//commands or sqls. parser starts here.
    /* empty */
    | commands command
    ;

command:
	  select  
	| insert
	| update
	| delete
	| create_table
	| drop_table
	| show_tables
	| desc_table
	| create_index	
	| drop_index
	| show_index
	| sync
	| begin
	| commit
	| rollback
	| load_data
	| help
	| exit
    ;

exit:			
    EXIT SEMICOLON {
        CONTEXT->ssql->flag=SCF_EXIT;//"exit";
    };

help:
    HELP SEMICOLON {
        CONTEXT->ssql->flag=SCF_HELP;//"help";
    };

sync:
    SYNC SEMICOLON {
      CONTEXT->ssql->flag = SCF_SYNC;
    }
    ;

begin:
    TRX_BEGIN SEMICOLON {
      CONTEXT->ssql->flag = SCF_BEGIN;
    }
    ;

commit:
    TRX_COMMIT SEMICOLON {
      CONTEXT->ssql->flag = SCF_COMMIT;
    }
    ;

rollback:
    TRX_ROLLBACK SEMICOLON {
      CONTEXT->ssql->flag = SCF_ROLLBACK;
    }
    ;

drop_table:		/*drop table 语句的语法解析树*/
    DROP TABLE ID SEMICOLON {
        CONTEXT->ssql->flag = SCF_DROP_TABLE;//"drop_table";
        drop_table_init(&CONTEXT->ssql->sstr.drop_table, $3);
    };

show_tables:
    SHOW TABLES SEMICOLON {
      CONTEXT->ssql->flag = SCF_SHOW_TABLES;
    }
    ;

show_index:
    SHOW INDEX FROM ID SEMICOLON {
    	CONTEXT->ssql->flag = SCF_SHOW_INDEX;
    	show_index_init(&CONTEXT->ssql->sstr.show_index, $4);
    };

desc_table:
    DESC ID SEMICOLON {
      CONTEXT->ssql->flag = SCF_DESC_TABLE;
      desc_table_init(&CONTEXT->ssql->sstr.desc_table, $2);
    }
    ;

create_index:		/*create index 语句的语法解析树*/
    CREATE INDEX ID ON ID LBRACE ID attrname_list RBRACE SEMICOLON
		{
			CONTEXT->ssql->flag = SCF_CREATE_INDEX;//"create_index";

			RelAttr attr;
			relation_attr_init(&attr, NULL, $7);
			create_indexs_append_attribute(&CONTEXT->ssql->sstr.create_index, &attr);

			create_index_init(&CONTEXT->ssql->sstr.create_index, $3, $5, 0);
		}
    | CREATE UNIQUE INDEX ID ON ID LBRACE ID attrname_list RBRACE SEMICOLON
    {
	CONTEXT->ssql->flag = SCF_CREATE_INDEX;//"create_index";

	RelAttr attr;
	relation_attr_init(&attr, NULL, $8);
	create_indexs_append_attribute(&CONTEXT->ssql->sstr.create_index, &attr);

	create_index_init(&CONTEXT->ssql->sstr.create_index, $4, $6, 1);
    }
    ;

attrname_list:
    /* empty */
    | COMMA ID attrname_list {
			RelAttr attr;
			relation_attr_init(&attr, NULL, $2);
			create_indexs_append_attribute(&CONTEXT->ssql->sstr.create_index, &attr);
      }
    ;

drop_index:			/*drop index 语句的语法解析树*/
    DROP INDEX ID  SEMICOLON 
		{
			CONTEXT->ssql->flag=SCF_DROP_INDEX;//"drop_index";
			drop_index_init(&CONTEXT->ssql->sstr.drop_index, $3);
		}
    ;
create_table:		/*create table 语句的语法解析树*/
    CREATE TABLE ID LBRACE attr_def attr_def_list RBRACE SEMICOLON 
		{
			CONTEXT->ssql->flag=SCF_CREATE_TABLE;//"create_table";
			// CONTEXT->ssql->sstr.create_table.attribute_count = CONTEXT->value_length;
			create_table_init_name(&CONTEXT->ssql->sstr.create_table, $3);
			//临时变量清零	
			CONTEXT->value_length = 0;
		}
    ;
attr_def_list:
    /* empty */
    | COMMA attr_def attr_def_list {    }
    ;
    
attr_def:
    ID_get type LBRACE number RBRACE null
		{
			AttrInfo attribute;
			attr_info_init(&attribute, CONTEXT->id, $2, $4);
			create_table_append_attribute(&CONTEXT->ssql->sstr.create_table, &attribute);
			// CONTEXT->ssql->sstr.create_table.attributes[CONTEXT->value_length].name =(char*)malloc(sizeof(char));
			// strcpy(CONTEXT->ssql->sstr.create_table.attributes[CONTEXT->value_length].name, CONTEXT->id); 
			// CONTEXT->ssql->sstr.create_table.attributes[CONTEXT->value_length].type = $2;  
			// CONTEXT->ssql->sstr.create_table.attributes[CONTEXT->value_length].length = $4;
			CONTEXT->value_length++;
		}
    |ID_get type null
		{
			AttrInfo attribute;
			attr_info_init(&attribute, CONTEXT->id, $2, 4);
			create_table_append_attribute(&CONTEXT->ssql->sstr.create_table, &attribute);
			// CONTEXT->ssql->sstr.create_table.attributes[CONTEXT->value_length].name=(char*)malloc(sizeof(char));
			// strcpy(CONTEXT->ssql->sstr.create_table.attributes[CONTEXT->value_length].name, CONTEXT->id); 
			// CONTEXT->ssql->sstr.create_table.attributes[CONTEXT->value_length].type=$2;  
			// CONTEXT->ssql->sstr.create_table.attributes[CONTEXT->value_length].length=4; // default attribute length
			CONTEXT->value_length++;
		}
    ;
null:
    | NULLABLE{

    }
    ;

number:
		NUMBER {$$ = $1;}
		;
type:
	INT_T { $$=INTS; }
       | STRING_T { $$=CHARS; }
       | FLOAT_T { $$=FLOATS; }
       | DATE_T { $$=DATES; }
       | TEXT_T { $$=TEXTS; }
       ;
ID_get:
	ID 
	{
		char *temp=$1; 
		snprintf(CONTEXT->id, sizeof(CONTEXT->id), "%s", temp);
	}
	;

	
insert:				/*insert   语句的语法解析树*/
    INSERT INTO ID VALUES tuple tuple_list SEMICOLON
		{
			// CONTEXT->values[CONTEXT->value_length++] = *$6;

			CONTEXT->ssql->flag=SCF_INSERT;//"insert";
			// CONTEXT->ssql->sstr.insertion.relation_name = $3;
			// CONTEXT->ssql->sstr.insertion.value_num = CONTEXT->value_length;
			// for(i = 0; i < CONTEXT->value_length; i++){
			// 	CONTEXT->ssql->sstr.insertion.values[i] = CONTEXT->values[i];
      // }
			inserts_init(&CONTEXT->ssql->sstr.insertion, $3, CONTEXT->tuples, CONTEXT->tuple_length);

      //临时变量清零
      CONTEXT->value_length=0;
      CONTEXT->tuple_length=0;
      memset(CONTEXT->tuples, 0, sizeof(CONTEXT->tuples));
    }

tuple_list:
    /* empty */
    | COMMA tuple tuple_list {
    };

tuple:
    /* empty */
    | LBRACE value value_list RBRACE {
    	memcpy(CONTEXT->tuples[CONTEXT->tuple_length].values, CONTEXT->values, sizeof(Value)*CONTEXT->value_length);
    	CONTEXT->tuples[CONTEXT->tuple_length].value_num = CONTEXT->value_length;
    	CONTEXT->value_length = 0;
        CONTEXT->tuple_length++;
    };

value_list:
    /* empty */
    | COMMA value value_list  { 
  		// CONTEXT->values[CONTEXT->value_length++] = *$2;
	  }
    ;
value:
    NUMBER{	
  		value_init_integer(&CONTEXT->values[CONTEXT->value_length++], $1);
		}
    |FLOAT{
  		value_init_float(&CONTEXT->values[CONTEXT->value_length++], $1);
		}
    |DATE_STR {
    		$1 = substr($1,1,strlen($1)-2);
                value_init_date(&CONTEXT->values[CONTEXT->value_length++], $1);
    }
    |SSS {
			$1 = substr($1,1,strlen($1)-2);
  		value_init_string(&CONTEXT->values[CONTEXT->value_length++], $1);
		}
    |NULL_V {
    	value_init_string(&CONTEXT->values[CONTEXT->value_length++], "null");
    }
    ;
    
delete:		/*  delete 语句的语法解析树*/
    DELETE FROM ID where SEMICOLON 
		{
			CONTEXT->ssql->flag = SCF_DELETE;//"delete";
			deletes_init_relation(&CONTEXT->ssql->sstr.deletion, $3);
			deletes_set_conditions(&CONTEXT->ssql->sstr.deletion, 
					CONTEXT->conditions, CONTEXT->condition_length);
			CONTEXT->condition_length = 0;	
    }
    ;
update:			/*  update 语句的语法解析树*/
    UPDATE ID SET ID EQ update_value set_list where SEMICOLON
		{
			CONTEXT->ssql->flag = SCF_UPDATE;//"update";

			RelAttr attr;
			relation_attr_init(&attr, NULL, $4);
			updates_append_attribute(&CONTEXT->ssql->sstr.update, &attr);

//			Value *value = &CONTEXT->values[0];
//			updates_append_value(&CONTEXT->ssql->sstr.update, value);

			updates_init(&CONTEXT->ssql->sstr.update, $2,
					CONTEXT->conditions, CONTEXT->condition_length);
			CONTEXT->condition_length = 0;
			CONTEXT->value_length = 0;
		}
    ;
set_list:
   /* empty */
   | COMMA ID EQ update_value set_list {
	RelAttr attr;
	relation_attr_init(&attr, NULL, $2);
	updates_append_attribute(&CONTEXT->ssql->sstr.update, &attr);
   }
   ;

update_value:
   value {
	Value *value = &CONTEXT->values[CONTEXT->value_length - 1];
	updates_append_value(&CONTEXT->ssql->sstr.update, value, NULL, 1);
   }
   | LBRACE select_in_update RBRACE {
	updates_append_value(&CONTEXT->ssql->sstr.update, NULL, &CONTEXT->selection, 0);
	memset(&CONTEXT->selection, 0, sizeof(Selects));
   }
   ;

select_in_update:
   SELECT select_attr FROM ID rel_list inner_join_list where
   {
   	selects_append_relation(&CONTEXT->selection, $4);

	selects_append_conditions(&CONTEXT->selection, CONTEXT->conditions, CONTEXT->condition_length);
	//临时变量清零
	CONTEXT->condition_length=0;
	CONTEXT->from_length=0;
	CONTEXT->select_length=0;
	CONTEXT->value_length = 0;
   }

select:				/*  select 语句的语法解析树*/
    SELECT select_attr FROM ID rel_list inner_join_list where orderbys SEMICOLON
		{
			// CONTEXT->ssql->sstr.selection.relations[CONTEXT->from_length++]=$4;
			selects_append_relation(&CONTEXT->selection, $4);

			selects_append_conditions(&CONTEXT->selection, CONTEXT->conditions, CONTEXT->condition_length);

			CONTEXT->ssql->flag=SCF_SELECT;//"select";
			// CONTEXT->ssql->sstr.selection.attr_num = CONTEXT->select_length;
			CONTEXT->ssql->sstr.selection = CONTEXT->selection;
			//临时变量清零
			CONTEXT->condition_length=0;
			CONTEXT->from_length=0;
			CONTEXT->select_length=0;
			CONTEXT->value_length = 0;
	}
	;

select_attr:
    STAR {  
			RelAttr attr;
			relation_attr_init(&attr, NULL, "*");
			selects_append_attribute(&CONTEXT->selection, &attr);
		}
    | STAR attr_list {
    			RelAttr attr;
    			relation_attr_init(&attr, NULL, "*");
    			selects_append_attribute(&CONTEXT->selection, &attr);
    		}
    | ID attr_list {
			RelAttr attr;
			relation_attr_init(&attr, NULL, $1);
			selects_append_attribute(&CONTEXT->selection, &attr);
		}
    | ID DOT ID attr_list {
			RelAttr attr;
			relation_attr_init(&attr, $1, $3);
			selects_append_attribute(&CONTEXT->selection, &attr);
		}
    | func LBRACE expression RBRACE attr_list { }
    ;
attr_list:
    /* empty */
    | COMMA ID attr_list {
			RelAttr attr;
			relation_attr_init(&attr, NULL, $2);
			selects_append_attribute(&CONTEXT->selection, &attr);
     	  // CONTEXT->ssql->sstr.selection.attributes[CONTEXT->select_length].relation_name = NULL;
        // CONTEXT->ssql->sstr.selection.attributes[CONTEXT->select_length++].attribute_name=$2;
      }
    | COMMA ID DOT ID attr_list {
			RelAttr attr;
			relation_attr_init(&attr, $2, $4);
			selects_append_attribute(&CONTEXT->selection, &attr);
        // CONTEXT->ssql->sstr.selection.attributes[CONTEXT->select_length].attribute_name=$4;
        // CONTEXT->ssql->sstr.selection.attributes[CONTEXT->select_length++].relation_name=$2;
  	  }
  	;
    | COMMA func LBRACE expression RBRACE attr_list {}

expression:
    STAR {// *
		RelAttr attr;
		relation_attr_init(&attr, NULL, "*");
		Aggregation aggr;
		aggr.attribute = attr;
		aggr.func_name = CONTEXT->func;
		aggr.is_value = 0;
		selects_append_aggregation(&CONTEXT->selection,&aggr);
	}
    | ID{
		RelAttr attr;
		relation_attr_init(&attr, NULL, $1);
		Aggregation aggr;
		aggr.attribute = attr;
		aggr.func_name = CONTEXT->func;
		aggr.is_value = 0;
		selects_append_aggregation(&CONTEXT->selection,&aggr);
        }
    | ID DOT ID{
		RelAttr attr;
		relation_attr_init(&attr, $1, $3);
		Aggregation aggr;
		aggr.attribute = attr;
		aggr.func_name = CONTEXT->func;
		aggr.is_value = 0;
		selects_append_aggregation(&CONTEXT->selection,&aggr);
	}
    | value{
		Aggregation aggr;
		aggr.func_name = CONTEXT->func;
		aggr.is_value = 1;
		aggr.value = CONTEXT->values[CONTEXT->value_length - 1];
		//CONTEXT->value_length = 0;
		selects_append_aggregation(&CONTEXT->selection,&aggr);
	}

inner_join_list:
    /* empty */
    | INNER JOIN ID ON condition condition_list inner_join_list {
    	selects_append_join(&CONTEXT->selection, $3);
    }
    ;

rel_list:
    /* empty */
    | COMMA ID rel_list {	
				selects_append_relation(&CONTEXT->selection, $2);
		  }
    ;
where:
    /* empty */ 
    | WHERE condition condition_list {	
				// CONTEXT->conditions[CONTEXT->condition_length++]=*$2;
			}
    ;
condition_list:
    /* empty */
    | AND condition condition_list {
				// CONTEXT->conditions[CONTEXT->condition_length++]=*$2;
			}
    ;
condition:
    ID comOp value
		{
			RelAttr left_attr;
			relation_attr_init(&left_attr, NULL, $1);

			Value *right_value = &CONTEXT->values[CONTEXT->value_length - 1];

			Condition condition;
			condition_init(&condition, CONTEXT->comp, 1, &left_attr, NULL, 0, NULL, right_value);
			CONTEXT->conditions[CONTEXT->condition_length++] = condition;
			// $$ = ( Condition *)malloc(sizeof( Condition));
			// $$->left_is_attr = 1;
			// $$->left_attr.relation_name = NULL;
			// $$->left_attr.attribute_name= $1;
			// $$->comp = CONTEXT->comp;
			// $$->right_is_attr = 0;
			// $$->right_attr.relation_name = NULL;
			// $$->right_attr.attribute_name = NULL;
			// $$->right_value = *$3;

		}
		|value comOp value 
		{
			Value *left_value = &CONTEXT->values[CONTEXT->value_length - 2];
			Value *right_value = &CONTEXT->values[CONTEXT->value_length - 1];

			Condition condition;
			condition_init(&condition, CONTEXT->comp, 0, NULL, left_value, 0, NULL, right_value);
			CONTEXT->conditions[CONTEXT->condition_length++] = condition;
			// $$ = ( Condition *)malloc(sizeof( Condition));
			// $$->left_is_attr = 0;
			// $$->left_attr.relation_name=NULL;
			// $$->left_attr.attribute_name=NULL;
			// $$->left_value = *$1;
			// $$->comp = CONTEXT->comp;
			// $$->right_is_attr = 0;
			// $$->right_attr.relation_name = NULL;
			// $$->right_attr.attribute_name = NULL;
			// $$->right_value = *$3;

		}
		|ID comOp ID 
		{
			RelAttr left_attr;
			relation_attr_init(&left_attr, NULL, $1);
			RelAttr right_attr;
			relation_attr_init(&right_attr, NULL, $3);

			Condition condition;
			condition_init(&condition, CONTEXT->comp, 1, &left_attr, NULL, 1, &right_attr, NULL);
			CONTEXT->conditions[CONTEXT->condition_length++] = condition;
			// $$=( Condition *)malloc(sizeof( Condition));
			// $$->left_is_attr = 1;
			// $$->left_attr.relation_name=NULL;
			// $$->left_attr.attribute_name=$1;
			// $$->comp = CONTEXT->comp;
			// $$->right_is_attr = 1;
			// $$->right_attr.relation_name=NULL;
			// $$->right_attr.attribute_name=$3;

		}
    |value comOp ID
		{
			Value *left_value = &CONTEXT->values[CONTEXT->value_length - 1];
			RelAttr right_attr;
			relation_attr_init(&right_attr, NULL, $3);

			Condition condition;
			condition_init(&condition, CONTEXT->comp, 0, NULL, left_value, 1, &right_attr, NULL);
			CONTEXT->conditions[CONTEXT->condition_length++] = condition;

			// $$=( Condition *)malloc(sizeof( Condition));
			// $$->left_is_attr = 0;
			// $$->left_attr.relation_name=NULL;
			// $$->left_attr.attribute_name=NULL;
			// $$->left_value = *$1;
			// $$->comp=CONTEXT->comp;
			
			// $$->right_is_attr = 1;
			// $$->right_attr.relation_name=NULL;
			// $$->right_attr.attribute_name=$3;
		
		}
    |ID DOT ID comOp value
		{
			RelAttr left_attr;
			relation_attr_init(&left_attr, $1, $3);
			Value *right_value = &CONTEXT->values[CONTEXT->value_length - 1];

			Condition condition;
			condition_init(&condition, CONTEXT->comp, 1, &left_attr, NULL, 0, NULL, right_value);
			CONTEXT->conditions[CONTEXT->condition_length++] = condition;

			// $$=( Condition *)malloc(sizeof( Condition));
			// $$->left_is_attr = 1;
			// $$->left_attr.relation_name=$1;
			// $$->left_attr.attribute_name=$3;
			// $$->comp=CONTEXT->comp;
			// $$->right_is_attr = 0;   //属性值
			// $$->right_attr.relation_name=NULL;
			// $$->right_attr.attribute_name=NULL;
			// $$->right_value =*$5;			
							
    }
    |value comOp ID DOT ID
		{
			Value *left_value = &CONTEXT->values[CONTEXT->value_length - 1];

			RelAttr right_attr;
			relation_attr_init(&right_attr, $3, $5);

			Condition condition;
			condition_init(&condition, CONTEXT->comp, 0, NULL, left_value, 1, &right_attr, NULL);
			CONTEXT->conditions[CONTEXT->condition_length++] = condition;
			// $$=( Condition *)malloc(sizeof( Condition));
			// $$->left_is_attr = 0;//属性值
			// $$->left_attr.relation_name=NULL;
			// $$->left_attr.attribute_name=NULL;
			// $$->left_value = *$1;
			// $$->comp =CONTEXT->comp;
			// $$->right_is_attr = 1;//属性
			// $$->right_attr.relation_name = $3;
			// $$->right_attr.attribute_name = $5;
									
    }
    |ID DOT ID comOp ID DOT ID
		{
			RelAttr left_attr;
			relation_attr_init(&left_attr, $1, $3);
			RelAttr right_attr;
			relation_attr_init(&right_attr, $5, $7);

			Condition condition;
			condition_init(&condition, CONTEXT->comp, 1, &left_attr, NULL, 1, &right_attr, NULL);
			CONTEXT->conditions[CONTEXT->condition_length++] = condition;
			// $$=( Condition *)malloc(sizeof( Condition));
			// $$->left_is_attr = 1;		//属性
			// $$->left_attr.relation_name=$1;
			// $$->left_attr.attribute_name=$3;
			// $$->comp =CONTEXT->comp;
			// $$->right_is_attr = 1;		//属性
			// $$->right_attr.relation_name=$5;
			// $$->right_attr.attribute_name=$7;
    }
    ;

orderbys:
	| ORDER BY ID orderby {
		RelAttr attr;
		relation_attr_init(&attr, NULL, $3);

		OrderBy order_by;
		order_by_init(&order_by, &attr, 1);
		selects_append_order_by(&CONTEXT->selection, &order_by);
	}
	| ORDER BY ID ASC orderby {
        	RelAttr attr;
		relation_attr_init(&attr, NULL, $3);

		OrderBy order_by;
		order_by_init(&order_by, &attr, 1);
		selects_append_order_by(&CONTEXT->selection, &order_by);
        }
	| ORDER BY ID DESC orderby {
		RelAttr attr;
		relation_attr_init(&attr, NULL, $3);

		OrderBy order_by;
		order_by_init(&order_by, &attr, 0);
		selects_append_order_by(&CONTEXT->selection, &order_by);
	}
	| ORDER BY ID DOT ID ASC orderby{
		RelAttr attr;
		relation_attr_init(&attr, $3, $5);

		OrderBy order_by;
		order_by_init(&order_by, &attr, 1);
		selects_append_order_by(&CONTEXT->selection, &order_by);
	}
	| ORDER BY ID DOT ID orderby{
		RelAttr attr;
		relation_attr_init(&attr, $3, $5);

		OrderBy order_by;
		order_by_init(&order_by, &attr, 1);
		selects_append_order_by(&CONTEXT->selection, &order_by);
	}
	| ORDER BY ID DOT ID DESC orderby{
		RelAttr attr;
		relation_attr_init(&attr, $3, $5);

		OrderBy order_by;
		order_by_init(&order_by, &attr, 0);
		selects_append_order_by(&CONTEXT->selection, &order_by);
	}
	;
orderby:
	| COMMA ID orderby{
		RelAttr attr;
		relation_attr_init(&attr, NULL, $2);

		OrderBy order_by;
		order_by_init(&order_by, &attr, 1);
		selects_append_order_by(&CONTEXT->selection, &order_by);
	}
	| COMMA ID ASC orderby{
		RelAttr attr;
		relation_attr_init(&attr, NULL, $2);

		OrderBy order_by;
		order_by_init(&order_by, &attr, 1);
		selects_append_order_by(&CONTEXT->selection, &order_by);
	}
	| COMMA ID DESC orderby {
		RelAttr attr;
		relation_attr_init(&attr, NULL, $2);

		OrderBy order_by;
		order_by_init(&order_by, &attr, 0);
		selects_append_order_by(&CONTEXT->selection, &order_by);
	}
	| COMMA ID DOT ID ASC orderby{
		RelAttr attr;
		relation_attr_init(&attr, $2, $4);

		OrderBy order_by;
		order_by_init(&order_by, &attr, 1);
		selects_append_order_by(&CONTEXT->selection, &order_by);
	}
	| COMMA ID DOT ID orderby{
		RelAttr attr;
		relation_attr_init(&attr, $2, $4);

		OrderBy order_by;
		order_by_init(&order_by, &attr, 1);
		selects_append_order_by(&CONTEXT->selection, &order_by);
	}
	| COMMA ID DOT ID DESC orderby{
		RelAttr attr;
		relation_attr_init(&attr, $2, $4);

		OrderBy order_by;
		order_by_init(&order_by, &attr, 0);
		selects_append_order_by(&CONTEXT->selection, &order_by);
	}
	;

comOp:
  	  EQ { CONTEXT->comp = EQUAL_TO; }
    | LT { CONTEXT->comp = LESS_THAN; }
    | GT { CONTEXT->comp = GREAT_THAN; }
    | LE { CONTEXT->comp = LESS_EQUAL; }
    | GE { CONTEXT->comp = GREAT_EQUAL; }
    | NE { CONTEXT->comp = NOT_EQUAL; }
    | NOT LIKE { CONTEXT->comp = STRING_NOT_LIKE; }
    | LIKE { CONTEXT->comp = STRING_LIKE; }
    ;

func:
  	AGGMAX { CONTEXT->func = AGG_MAX; }
    | AGGMIN { CONTEXT->func = AGG_MIN; }
    | AGGCOUNT { CONTEXT->func = AGG_COUNT; }
    | AGGAVG { CONTEXT->func = AGG_AVG; }
    | AGGSUM { CONTEXT->func = AGG_SUM; }
    ;

load_data:
		LOAD DATA INFILE SSS INTO TABLE ID SEMICOLON
		{
		  CONTEXT->ssql->flag = SCF_LOAD_DATA;
			load_data_init(&CONTEXT->ssql->sstr.load_data, $7, $4);
		}
		;
%%
//_____________________________________________________________________
extern void scan_string(const char *str, yyscan_t scanner);

int sql_parse(const char *s, Query *sqls){
	ParserContext context;
	memset(&context, 0, sizeof(context));

	yyscan_t scanner;
	yylex_init_extra(&context, &scanner);
	context.ssql = sqls;
	scan_string(s, scanner);
	int result = yyparse(scanner);
	yylex_destroy(scanner);
	return result;
}
