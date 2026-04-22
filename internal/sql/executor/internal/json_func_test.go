package internal

import (
	"testing"
)

// TestExec_JsonExtract tests JSON_EXTRACT function.
func TestExec_JsonExtract(t *testing.T) {
	env := newTestEnv(t)
	defer env.store.Close()

	env.execSQL(t, "CREATE TABLE t (id INT PRIMARY KEY, js TEXT)")

	t.Run("basic_object_extract", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_EXTRACT('{\"name\":\"Alice\",\"age\":30}', '$.name')")
		if result.Rows[0][0].Text != `"Alice"` {
			t.Errorf("JSON_EXTRACT = %q, want %q", result.Rows[0][0].Text, `"Alice"`)
		}
	})

	t.Run("numeric_value", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_EXTRACT('{\"name\":\"Alice\",\"age\":30}', '$.age')")
		if result.Rows[0][0].Text != `30` {
			t.Errorf("JSON_EXTRACT = %q, want %q", result.Rows[0][0].Text, `30`)
		}
	})

	t.Run("nested_object", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_EXTRACT('{\"user\":{\"name\":\"Bob\"}}', '$.user.name')")
		if result.Rows[0][0].Text != `"Bob"` {
			t.Errorf("JSON_EXTRACT = %q, want %q", result.Rows[0][0].Text, `"Bob"`)
		}
	})

	t.Run("array_element", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_EXTRACT('[1,2,3]', '$[0]')")
		if result.Rows[0][0].Text != `1` {
			t.Errorf("JSON_EXTRACT = %q, want %q", result.Rows[0][0].Text, `1`)
		}
	})

	t.Run("array_second_element", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_EXTRACT('[1,2,3]', '$[1]')")
		if result.Rows[0][0].Text != `2` {
			t.Errorf("JSON_EXTRACT = %q, want %q", result.Rows[0][0].Text, `2`)
		}
	})

	t.Run("array_object_access", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_EXTRACT('[{\"name\":\"Test\"}]', '$[0].name')")
		if result.Rows[0][0].Text != `"Test"` {
			t.Errorf("JSON_EXTRACT = %q, want %q", result.Rows[0][0].Text, `"Test"`)
		}
	})

	t.Run("path_not_found", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		// jsonNavigate returns nil for non-existent paths
		result := env.execSQL(t, "SELECT JSON_EXTRACT('{\"a\":1}', '$.b')")
		if result.Rows[0][0].Text != `` {
			t.Errorf("JSON_EXTRACT for non-existent path = %q, want empty", result.Rows[0][0].Text)
		}
	})

	t.Run("index_out_of_range", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_EXTRACT('[1,2]', '$[10]')")
		if result.Rows[0][0].Text != `` {
			t.Errorf("JSON_EXTRACT for out-of-range index = %q, want empty", result.Rows[0][0].Text)
		}
	})

	t.Run("null_json", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_EXTRACT(NULL, '$.name')")
		if !result.Rows[0][0].IsNull {
			t.Errorf("JSON_EXTRACT(NULL, ...) = %v, want NULL", result.Rows[0][0])
		}
	})

	t.Run("null_path", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_EXTRACT('{\"name\":\"test\"}', NULL)")
		if !result.Rows[0][0].IsNull {
			t.Errorf("JSON_EXTRACT(..., NULL) = %v, want NULL", result.Rows[0][0])
		}
	})

	t.Run("root_path", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_EXTRACT('{\"a\":1}', '$')")
		if result.Rows[0][0].Text != `{"a":1}` {
			t.Errorf("JSON_EXTRACT with root path = %q, want %q", result.Rows[0][0].Text, `{"a":1}`)
		}
	})

	t.Run("boolean_value", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_EXTRACT('{\"flag\":true}', '$.flag')")
		if result.Rows[0][0].Text != `true` {
			t.Errorf("JSON_EXTRACT = %q, want %q", result.Rows[0][0].Text, `true`)
		}
	})

	t.Run("false_value", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_EXTRACT('{\"flag\":false}', '$.flag')")
		if result.Rows[0][0].Text != `false` {
			t.Errorf("JSON_EXTRACT = %q, want %q", result.Rows[0][0].Text, `false`)
		}
	})

	t.Run("null_value_in_json", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_EXTRACT('{\"a\":null}', '$.a')")
		if result.Rows[0][0].Text != `null` {
			t.Errorf("JSON_EXTRACT = %q, want %q", result.Rows[0][0].Text, `null`)
		}
	})
}

// TestExec_JsonType tests JSON_TYPE function.
func TestExec_JsonType(t *testing.T) {
	env := newTestEnv(t)
	defer env.store.Close()

	env.execSQL(t, "CREATE TABLE t (id INT PRIMARY KEY, js TEXT)")

	t.Run("text_type", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_TYPE('{\"name\":\"Alice\"}', '$.name')")
		if result.Rows[0][0].Text != "text" {
			t.Errorf("JSON_TYPE = %q, want 'text'", result.Rows[0][0].Text)
		}
	})

	t.Run("integer_type", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_TYPE('{\"age\":30}', '$.age')")
		if result.Rows[0][0].Text != "integer" {
			t.Errorf("JSON_TYPE = %q, want 'integer'", result.Rows[0][0].Text)
		}
	})

	t.Run("real_type", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_TYPE('{\"pi\":3.14}', '$.pi')")
		if result.Rows[0][0].Text != "real" {
			t.Errorf("JSON_TYPE = %q, want 'real'", result.Rows[0][0].Text)
		}
	})

	t.Run("array_type", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_TYPE('[1,2,3]', '$')")
		if result.Rows[0][0].Text != "array" {
			t.Errorf("JSON_TYPE = %q, want 'array'", result.Rows[0][0].Text)
		}
	})

	t.Run("object_type", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_TYPE('{\"a\":1}', '$')")
		if result.Rows[0][0].Text != "object" {
			t.Errorf("JSON_TYPE = %q, want 'object'", result.Rows[0][0].Text)
		}
	})

	t.Run("null_type", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_TYPE('{\"a\":null}', '$.a')")
		if result.Rows[0][0].Text != "null" {
			t.Errorf("JSON_TYPE = %q, want 'null'", result.Rows[0][0].Text)
		}
	})

	t.Run("null_type_for_missing_path", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_TYPE('{\"a\":1}', '$.b')")
		if result.Rows[0][0].Text != "null" {
			t.Errorf("JSON_TYPE for missing path = %q, want 'null'", result.Rows[0][0].Text)
		}
	})

	t.Run("boolean_type", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_TYPE('{\"flag\":true}', '$.flag')")
		if result.Rows[0][0].Text != "integer" {
			t.Errorf("JSON_TYPE for boolean = %q, want 'integer' (SQLite compatibility)", result.Rows[0][0].Text)
		}
	})

	t.Run("null_json", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_TYPE(NULL, '$')")
		if !result.Rows[0][0].IsNull {
			t.Errorf("JSON_TYPE(NULL, ...) = %v, want NULL", result.Rows[0][0])
		}
	})
}
// TestExec_JsonSetInsertRemove tests JSON_SET, JSON_INSERT, and JSON_REMOVE functions.
func TestExec_JsonSetInsertRemove(t *testing.T) {
	env := newTestEnv(t)
	defer env.store.Close()

	env.execSQL(t, "CREATE TABLE t (id INT PRIMARY KEY, js TEXT)")

	t.Run("json_set_update_existing", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_SET('{\"a\":1}', '$.a', 2)")
		if result.Rows[0][0].Text != `{"a":2}` {
			t.Errorf("JSON_SET = %q, want %q", result.Rows[0][0].Text, `{"a":2}`)
		}
	})

	t.Run("json_set_add_new_key", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_SET('{\"a\":1}', '$.b', 2)")
		if result.Rows[0][0].Text != `{"a":1,"b":2}` {
			t.Errorf("JSON_SET = %q, want %q", result.Rows[0][0].Text, `{"a":1,"b":2}`)
		}
	})

	t.Run("json_set_nested_path", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_SET('{\"user\":{\"name\":\"Alice\"}}', '$.user.name', 'Bob')")
		if result.Rows[0][0].Text != `{"user":{"name":"Bob"}}` {
			t.Errorf("JSON_SET = %q, want %q", result.Rows[0][0].Text, `{"user":{"name":"Bob"}}`)
		}
	})

	t.Run("json_set_array_element", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_SET('[1,2,3]', '$[1]', 20)")
		if result.Rows[0][0].Text != `[1,20,3]` {
			t.Errorf("JSON_SET = %q, want %q", result.Rows[0][0].Text, `[1,20,3]`)
		}
	})

	t.Run("json_insert_existing_key_no_change", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_INSERT('{\"a\":1}', '$.a', 2)")
		if result.Rows[0][0].Text != `{"a":1}` {
			t.Errorf("JSON_INSERT = %q, want %q", result.Rows[0][0].Text, `{"a":1}`)
		}
	})

	t.Run("json_insert_new_key", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_INSERT('{\"a\":1}', '$.b', 2)")
		if result.Rows[0][0].Text != `{"a":1,"b":2}` {
			t.Errorf("JSON_INSERT = %q, want %q", result.Rows[0][0].Text, `{"a":1,"b":2}`)
		}
	})

	t.Run("json_remove_existing_key", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_REMOVE('{\"a\":1,\"b\":2}', '$.b')")
		if result.Rows[0][0].Text != `{"a":1}` {
			t.Errorf("JSON_REMOVE = %q, want %q", result.Rows[0][0].Text, `{"a":1}`)
		}
	})

	t.Run("json_remove_array_element", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_REMOVE('[1,2,3]', '$[1]')")
		if result.Rows[0][0].Text != `[1,3]` {
			t.Errorf("JSON_REMOVE = %q, want %q", result.Rows[0][0].Text, `[1,3]`)
		}
	})

	t.Run("json_set_null_json", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_SET(NULL, '$.a', 1)")
		if !result.Rows[0][0].IsNull {
			t.Errorf("JSON_SET with NULL should return NULL")
		}
	})

	t.Run("json_remove_null_json", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		result := env.execSQL(t, "SELECT JSON_REMOVE(NULL, '$.a')")
		if !result.Rows[0][0].IsNull {
			t.Errorf("JSON_REMOVE with NULL should return NULL")
		}
	})
}
// TestExec_JsonFuncWithColumns tests JSON functions applied to table columns.
func TestExec_JsonFuncWithColumns(t *testing.T) {
	env := newTestEnv(t)
	defer env.store.Close()

	env.execSQL(t, "CREATE TABLE data (id INT PRIMARY KEY, json_col TEXT)")
	env.execSQL(t, "INSERT INTO data VALUES (1, '{\"name\":\"Alice\",\"score\":95}')")
	env.execSQL(t, "INSERT INTO data VALUES (2, '{\"name\":\"Bob\",\"score\":87}')")

	t.Run("extract_from_column", func(t *testing.T) {
		result := env.execSQL(t, "SELECT JSON_EXTRACT(json_col, '$.name') FROM data ORDER BY id")
		if result.Rows[0][0].Text != `"Alice"` {
			t.Errorf("JSON_EXTRACT from column = %q, want %q", result.Rows[0][0].Text, `"Alice"`)
		}
		if result.Rows[1][0].Text != `"Bob"` {
			t.Errorf("JSON_EXTRACT from column = %q, want %q", result.Rows[1][0].Text, `"Bob"`)
		}
	})

	t.Run("type_from_column", func(t *testing.T) {
		result := env.execSQL(t, "SELECT JSON_TYPE(json_col, '$.name') FROM data ORDER BY id")
		if result.Rows[0][0].Text != "text" {
			t.Errorf("JSON_TYPE from column = %q, want 'text'", result.Rows[0][0].Text)
		}
	})
}