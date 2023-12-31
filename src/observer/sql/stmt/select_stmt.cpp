/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/6/6.
//

#include <algorithm>
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "common/log/log.h"
#include "common/lang/string.h"
#include "storage/common/db.h"
#include "storage/common/table.h"

SelectStmt::~SelectStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

static void wildcard_fields(Table *table, std::vector<Field> &field_metas)
{
  const TableMeta &table_meta = table->table_meta();
  const int field_num = table_meta.field_num();
  for (int i = table_meta.sys_field_num(); i < field_num; i++) {
    field_metas.push_back(Field(table, table_meta.field(i)));
  }
}

RC SelectStmt::create(Db *db, const Selects &select_sql, Stmt *&stmt)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  // collect tables in `inner join` statement
  std::vector<Table *> tables;
  std::unordered_map<std::string, Table *> table_map;
  for (size_t i = 0; i < select_sql.join_relation_num; i ++) {
    const char *table_name = select_sql.join_relations[i];
    if (nullptr == table_name) {
      LOG_WARN("invalid argument. relation name is null. index=%d", i);
      return RC::INVALID_ARGUMENT;
    }
    if (table_map.find(table_name) != table_map.end()) { // from statement appeared
      LOG_WARN("invalid argument. FROM statement appeared. table name=%s", table_name);
      return RC::INVALID_ARGUMENT;
    }
    Table *table = db->find_table(table_name);
    if (nullptr == table) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    tables.push_back(table);
    table_map.insert(std::pair<std::string, Table*>(table_name, table));
  }
  // collect tables in `from` statement
  for (size_t i = 0; i < select_sql.relation_num; i++) {
    const char *table_name = select_sql.relations[i];
    if (nullptr == table_name) {
      LOG_WARN("invalid argument. relation name is null. index=%d", i);
      return RC::INVALID_ARGUMENT;
    }

    Table *table = db->find_table(table_name);
    if (nullptr == table) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    tables.push_back(table);
    table_map.insert(std::pair<std::string, Table*>(table_name, table));
  }
  std::reverse(tables.begin(), tables.end());

  // check aggregation and single
  if (select_sql.attr_num && select_sql.aggregation_num) {
    LOG_WARN("aggregation and single field are redundant");
    return RC::SCHEMA_FIELD_REDUNDAN;
  }

  // collect query fields in `select` statement
  std::vector<Field> query_fields_forprint;
  std::vector<Field> query_fields;
  if (select_sql.attr_num) {
    for (int i = select_sql.attr_num - 1; i >= 0; i--) {
      const RelAttr &relation_attr = select_sql.attributes[i];

      if (common::is_blank(relation_attr.relation_name) && 0 == strcmp(relation_attr.attribute_name, "*")) {
        for (Table *table : tables) {
          wildcard_fields(table, query_fields_forprint);
        }

      } else if (!common::is_blank(relation_attr.relation_name)) { // TODO
        const char *table_name = relation_attr.relation_name;
        const char *field_name = relation_attr.attribute_name;

        if (0 == strcmp(table_name, "*")) {
          if (0 != strcmp(field_name, "*")) {
            LOG_WARN("invalid field name while table is *. attr=%s", field_name);
            return RC::SCHEMA_FIELD_MISSING;
          }
          for (Table *table : tables) {
            wildcard_fields(table, query_fields_forprint);
          }
        } else {
          auto iter = table_map.find(table_name);
          if (iter == table_map.end()) {
            LOG_WARN("no such table in from list: %s", table_name);
            return RC::SCHEMA_FIELD_MISSING;
          }

          Table *table = iter->second;
          if (0 == strcmp(field_name, "*")) {
            wildcard_fields(table, query_fields_forprint);
          } else {
            const FieldMeta *field_meta = table->table_meta().field(field_name);
            if (nullptr == field_meta) {
              LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), field_name);
              return RC::SCHEMA_FIELD_MISSING;
            }

            query_fields_forprint.push_back(Field(table, field_meta));
          }
        }
      } else {
        if (tables.size() != 1) {
          LOG_WARN("invalid. I do not know the attr's table. attr=%s", relation_attr.attribute_name);
          return RC::SCHEMA_FIELD_MISSING;
        }

        Table *table = tables[0];
        const FieldMeta *field_meta = table->table_meta().field(relation_attr.attribute_name);
        if (nullptr == field_meta) {
          LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), relation_attr.attribute_name);
          return RC::SCHEMA_FIELD_MISSING;
        }

        query_fields_forprint.push_back(Field(table, field_meta));
      }
    }

    if (tables.size() == 1) {
      query_fields = query_fields_forprint;
    } else {
      for (Table *table : tables) {
        wildcard_fields(table, query_fields);
      }
    }
  }

  // collect query fields(aggregation) in `select` statement
  std::vector<Aggregation> aggregations;
  if (select_sql.aggregation_num) {
    for (int i = 0; i < select_sql.aggregation_num; i ++) {
      if (strcmp(select_sql.aggregations[i].attribute.attribute_name, "*") == 0 &&
          select_sql.aggregations[i].func_name != FuncName::AGG_COUNT) {
        return RC::MISMATCH;
      }
      aggregations.push_back(select_sql.aggregations[i]);
    }
    for (Table *table : tables) {
      wildcard_fields(table, query_fields);
    }
  }

  LOG_INFO("got %d tables in from stmt and %d fields in query stmt", tables.size(), query_fields_forprint.size());

  Table *default_table = nullptr;
  if (tables.size() == 1) {
    default_table = tables[0];
  }

  // create filter statement in `where` statement
  FilterStmt *filter_stmt = nullptr;
  RC rc = FilterStmt::create(db, default_table, &table_map,
           select_sql.conditions, select_sql.condition_num, filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }

  // check order by
  std::vector<OrderBy> orderbys;
  for (size_t i = 0; i < select_sql.order_by_num; i ++) {
    const RelAttr &relation_attr = select_sql.order_bys[i].order_by_attr;
    Table *table = nullptr;
    if (!common::is_blank(relation_attr.relation_name)) {
      auto iter = table_map.find(relation_attr.relation_name);
      if (iter == table_map.end()) {
        LOG_WARN("no such table in from list: %s", relation_attr.relation_name);
        return RC::SCHEMA_FIELD_MISSING;
      }
      table = iter->second;
    }
    if (table == nullptr) {
      table = tables[0];
    }
    const FieldMeta *field_meta = table->table_meta().field(relation_attr.attribute_name);
    if (nullptr == field_meta) {
      LOG_WARN("no such field. field=%s", relation_attr.attribute_name);
      return RC::SCHEMA_FIELD_MISSING;
    }
    orderbys.push_back(select_sql.order_bys[i]);
  }

  // everything alright
  SelectStmt *select_stmt = new SelectStmt();
  select_stmt->table_map_.swap(table_map);
  select_stmt->tables_.swap(tables);
  select_stmt->is_inner_join_ = select_sql.join_relation_num > 0;
  select_stmt->query_fields_.swap(query_fields);
  select_stmt->query_fields_forprint_.swap(query_fields_forprint);
  select_stmt->filter_stmt_ = filter_stmt;
  select_stmt->aggregations_.swap(aggregations);
  select_stmt->orderbys_.swap(orderbys);
  stmt = select_stmt;
  return RC::SUCCESS;
}
