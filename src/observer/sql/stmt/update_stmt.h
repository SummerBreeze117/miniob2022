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

#pragma once

#include "rc.h"
#include "sql/stmt/stmt.h"
#include "storage/common/field.h"

class Table;
class FilterStmt;

class UpdateStmt : public Stmt
{
public:

  UpdateStmt() = default;
  UpdateStmt(Table *table, FilterStmt *filter_stmt);
  StmtType type() const override { return StmtType::UPDATE; }

public:
  static RC create(Db *db, const Updates &update_sql, Stmt *&stmt);

public:
  Table *table() const { return table_; }
  std::vector<UpdateValue>& updateValues()  { return updateValues_; }
  std::vector<Value>& values() { return values_; }
  void setValues(std::vector<Value> values) { values.swap(values_); }
  std::vector<const FieldMeta *>& fields()  { return fields_; }
  FilterStmt *filter_stmt() const { return filter_stmt_; }

private:
  Table *table_ = nullptr;
  std::vector<const FieldMeta *> fields_;
  std::vector<Value> values_;
  std::vector<UpdateValue> updateValues_;
  FilterStmt *filter_stmt_ = nullptr;
};

