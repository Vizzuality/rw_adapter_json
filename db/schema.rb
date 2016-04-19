# encoding: UTF-8
# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# Note that this schema.rb definition is the authoritative source for your
# database schema. If you need to create the application database on another
# system, you should be using db:schema:load, not running all the migrations
# from scratch. The latter is a flawed and unsustainable approach (the more migrations
# you'll amass, the slower it'll run and the greater likelihood for issues).
#
# It's strongly recommended that you check this file into your version control system.

ActiveRecord::Schema.define(version: 20160411161037) do

  # These are extensions that must be enabled in order to support this database
  enable_extension "plpgsql"
  enable_extension "citext"

  create_table "datasets", force: :cascade do |t|
    t.jsonb    "data_columns",  default: []
    t.jsonb    "data",          default: {}
    t.integer  "format",        default: 0
    t.integer  "row_count"
    t.integer  "dateable_id"
    t.string   "dateable_type"
    t.datetime "created_at",                 null: false
    t.datetime "updated_at",                 null: false
  end

  add_index "datasets", ["data"], name: "index_datasets_on_data", using: :gin
  add_index "datasets", ["dateable_id", "dateable_type"], name: "index_datasets_on_connector_and_connector_type", unique: true, using: :btree

  create_table "json_connectors", force: :cascade do |t|
    t.string   "connector_name"
    t.integer  "connector_format",          default: 0
    t.string   "connector_path"
    t.string   "attributes_path"
    t.integer  "connector_provider",        default: 0
    t.string   "parent_connector_url"
    t.integer  "parent_connector_id"
    t.string   "parent_connector_type"
    t.string   "parent_connector_provider"
    t.datetime "created_at",                            null: false
    t.datetime "updated_at",                            null: false
  end

  create_table "rest_connectors", force: :cascade do |t|
    t.string   "connector_name"
    t.string   "connector_url"
    t.integer  "connector_format",   default: 0
    t.string   "connector_path"
    t.integer  "connector_provider", default: 0
    t.string   "attributes_path"
    t.string   "table_name"
    t.datetime "created_at",                     null: false
    t.datetime "updated_at",                     null: false
  end

end
