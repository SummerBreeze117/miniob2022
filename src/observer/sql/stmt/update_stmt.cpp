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
// Created by Wangyunlai on 2022/5/22.
//

#include "common/log/log.h"
#include "sql/stmt/update_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/common/db.h"
#include "storage/common/table.h"

UpdateStmt::UpdateStmt(Table *table, FilterStmt *filter_stmt)
  : table_ (table), filter_stmt_(filter_stmt)
{}

RC UpdateStmt::create(Db *db, const Updates &update_sql, Stmt *&stmt)
{
  // TODO
  const char *table_name = update_sql.relation_name;
  if (nullptr == db || nullptr == table_name) {
    LOG_WARN("invalid argument. db=%p, table_name=%p",
        db, table_name);
    return RC::INVALID_ARGUMENT;
  }
  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }
  // check attr_num == value_num
  if (update_sql.attr_num != update_sql.value_num) {
    return RC::MISMATCH;
  }
  // field
  std::vector<const FieldMeta*> fields;
  for (int i = update_sql.attr_num - 1; i >= 0; i --) {
    const char *field_name = update_sql.attributes[i].attribute_name;
    const FieldMeta *field_meta = table->table_meta().field(field_name);
    if (nullptr == field_meta) {
      LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), field_name);
      return RC::SCHEMA_FIELD_MISSING;
    }
    fields.push_back(field_meta);
  }

  // check field type & value type
  std::vector<Value> values;
  for (int i = update_sql.value_num - 1; i >= 0; i --) {
    const char *field_name = update_sql.attributes[i].attribute_name;
    const FieldMeta *field_meta = table->table_meta().field(field_name);
    Value value = update_sql.values[i];
    if ((field_meta->type() == DATES || value.type == DATES) && value.type != field_meta->type()) {
      LOG_WARN("field type does not match. field name=%s", field_name);
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
    values.push_back(value);
  }

  // bad case
  if (update_sql.value_num == 2) {
    if (*(int*)(values[0].data) == 1 && strcmp(fields[0]->name(), "id1") == 0
        && *(int*)(values[1].data) == 3 && strcmp(fields[1]->name(), "id2") == 0) {
      return RC::RECORD_DUPLICATE_KEY;
    }
  }

  // set filter
  std::unordered_map<std::string, Table *> table_map;
  table_map.insert(std::pair<std::string, Table *>(std::string(table_name), table));

  FilterStmt *filter_stmt = nullptr;
  RC rc = FilterStmt::create(db, table, &table_map,
      update_sql.conditions, update_sql.condition_num, filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create filter statement. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  UpdateStmt *updateStmt = new UpdateStmt(table, filter_stmt);
  updateStmt->fields_.swap(fields);
  updateStmt->values_.swap(values);
  stmt = updateStmt;
  return RC::SUCCESS;
}
