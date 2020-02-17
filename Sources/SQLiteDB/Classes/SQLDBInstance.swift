//
//  SQLDBInstance.swift
//  DBLib
//
//  Created by Matt Hogg on 16/11/2019.
//  Copyright © 2019 Matthew Hogg. All rights reserved.
//

import Foundation
import SQLite3
import LoggingFramework



public class SQLDBInstance : BaseIndentLog {
	
	public typealias SQLitePointer = OpaquePointer
		
	public override init() {
		super.init()
		_log = self
	}
	
	public init(log: IIndentLog?) {
		_log = log
	}
	private var _log : IIndentLog?

	private var _db : OpaquePointer? = nil
	
	public func allTables() -> [String] {
		return allTables { (row) in
			
		}
	}
	public func allTables(rowHandler: (SQLRow) -> Void) -> [String] {
		let sql = "SELECT * FROM sqlite_master WHERE type = 'table'"
		var ret : [String] = []
		multiRow(rowHandler: { (row) in
			ret.append(row.get("name", ""))
			rowHandler(row)
		}, sql)
		return ret
	}
	
	public func columnInfo(_ tableName: String) -> [String] {
		return columnInfo(tableName) { (row) in
			
		}
	}
	
	public func columnInfo(_ tableName: String, rowHandler: (SQLRow) -> Void) -> [String] {
		let sql = "PRAGMA TABLE_INFO ([\(tableName)])"
		var ret : [String] = []
		multiRow(rowHandler: { (row) in
			ret.append(row.get("name", ""))
			rowHandler(row)
		}, sql)
		return ret
	}
	
	public func allIndexes() -> [String] {
		return allIndexes { (row) in
			
		}
	}
	public func allIndexes(rowHandler: (SQLRow) -> Void) -> [String] {
		let sql = "SELECT * FROM sqlite_master WHERE type = 'index'"
		var ret : [String] = []
		multiRow(rowHandler: { (row) in
			ret.append(row.get("name", ""))
			rowHandler(row)
		}, sql)
		return ret
	}
	
	public func tableExists(_ name: String) -> Bool {
		let sql = "SELECT count(*) FROM sqlite_master WHERE type='table' AND name LIKE ?;"
		let result = queryValue(sql, 0, name)
		return result > 0
	}
	
	public func indexExists(_ name: String) -> Bool {
		let table = name.replacingOccurrences(of: "[", with: "").replacingOccurrences(of: "]", with: "").trim()
		let sql = "SELECT count(*) FROM sqlite_master WHERE type='index' AND name LIKE ?;"
		let result = queryValue(sql, "", table)
		if !result.isOneOf("0", "") {
			return true
		}
		return false
	}
	
	public func open(path: URL, success: () -> Void = {}) {
		open(path: path, openCurrent: false, success: success)
	}
	public func open(path: URL, openCurrent: Bool = false, success: () -> Void = {}) {
		open(path: path.path, openCurrent: openCurrent, success: success)
	}
	public func open(success: () -> Void = {}) {
		open(openCurrent: false, success: success)
	}
	public func open(openCurrent : Bool = false, success: () -> Void = {}) {
		open(path: "", openCurrent: openCurrent, success: success)
	}
	public func open(path: String, success: () -> Void = {}) {
		open(path: path, openCurrent: false, success: success)
	}
	public func open(path: String, openCurrent : Bool = false, success: () -> Void = {}) {
		close()
		
		var sqlPath = path
		
		if path == "" {
			let fm = FileManager.default
			let docsurl = try! fm.url(for:.documentDirectory, in: .userDomainMask, appropriateFor: nil, create: false)
			sqlPath = String(describing: docsurl) + "data.sqlite"
		}
		
		//Archive?
		if !openCurrent && FileManager.default.fileExists(atPath: path) {
			let fm = FileManager.default
			var backup = 0
			while (fm.fileExists(atPath: path + " [\(backup)]")) {
				backup += 1
			}
			do {
				_log.debug("Backup \(path) to [\(backup)]")
				try fm.copyItem(atPath: path, toPath: path + " [\(backup)]")
				try fm.removeItem(atPath: path)
			}
			catch {
				
			}
		}
		_path = sqlPath
		success()
		return
	}
	
	private func openDB() -> OpaquePointer? {
		var db : OpaquePointer? = nil
		if sqlite3_open(_path, &db) == SQLITE_OK {
		}
		return db
	}
	
	private func closeDB(_ db: OpaquePointer?) {
		if db != nil {
			sqlite3_close(db!)
		}
	}
	
	private var _path : String = ""
	
	public func close() {
		if let db = _db {
			sqlite3_close(db)
		}
		_db = nil
	}
	
	public func queryList<T>(_ sql: String, column: String, hintValue: T, parms: Any?...) -> [T] {
		var ret: [T] = []
		self.multiRow(rowHandler: { (row) in
			ret.append(row.get(column, hintValue))
		}, sql, parms)
		return ret
	}
	
	public func queryList<T>(_ sql: String, columnIndex: Int, hintValue: T, parms: Any?...) -> [T] {

		var ret: [T] = []
		self.multiRow(rowHandler: { (row) in
			ret.append(row.get(columnIndex, hintValue))
		}, sql, parms)
		return ret
	}

	public func queryList<T>(_ sql: String, hintValue: T, parms: Any?...) -> [T] {
		return queryList(sql, columnIndex: 0, hintValue: hintValue, parms: parms)
	}
	
	public func queryValue<T>(_ sql: String, _ defaultValue: T, _ parms: Any?...) -> T {
		let _db = openDB()
		if _db == nil {
			return defaultValue
		}
		defer {
			closeDB(_db)
		}
		var statement : SQLitePointer? = nil
		var ret = defaultValue
		if sqlite3_prepare(_db, sql, -1, &statement, nil) == SQLITE_OK {
			bindParameters(statement: statement, parameters: parms)
			
			//shared.SQL(sql)
			
			var step = sqlite3_step(statement)
			while step != SQLITE_DONE {
				if step == SQLITE_ROW {
					
					if defaultValue is Int {
						if let r = sqlite3_column_int(statement, 0) as? T {
							ret = r
						}
						else {
							//Ah bugger. Might be an int64 instead!!
							if let r64 = sqlite3_column_int64(statement, 0) as? Int64 {
								ret = Int(r64) as! T
							}
							else {
								ret = defaultValue
							}
						}
					}
					else {
						if defaultValue is Int64 {
							ret = sqlite3_column_int64(statement, 0) as! T
						}
						else {
							let sv = String(cString: sqlite3_column_text(statement, 0))
							if defaultValue is Bool {
								ret = sv.toBool() as! T
							} else {
								ret = sv as! T
							}
						}
					}
					
					break
				}
				step = sqlite3_step(statement)
			}
		}
		else {
			let errmsg = String(cString: sqlite3_errmsg(_db))
			print("E: " + errmsg)
		}
		sqlite3_finalize(statement)
		return ret
	}
	
	private func lastInsertedRowID(_ _db: OpaquePointer?) -> Int {
		if _db == nil {
			return -1
		}
		var statement : SQLitePointer? = nil
		var ret = -1
		if sqlite3_prepare(_db, "SELECT last_insert_rowid()", -1, &statement, nil) == SQLITE_OK {
			
			var step = sqlite3_step(statement)
			while step != SQLITE_DONE {
				if step == SQLITE_ROW {
					
					if let r = sqlite3_column_int(statement, 0) as? Int32 {
						ret = Int(r) // as! Int
					}
					else {
						//Ah bugger. Might be an int64 instead!!
						if let r64 = sqlite3_column_int64(statement, 0) as? Int64 {
							ret = Int(r64) // as! Int
						}
						else {
							ret = -1
						}
					}
					
					break
				}
				step = sqlite3_step(statement)
			}
		}
		else {
			let errmsg = String(cString: sqlite3_errmsg(_db))
			print("E: " + errmsg)
		}
		sqlite3_finalize(statement)
		return ret
	}
	
	
	public func assertIndex(indexName: String, createSql: String) {
		if !indexExists(indexName) {
			execute(createSql)
		}
	}
	
	/// Creates an index against a table and column(s)
	///
	/// - Parameters:
	///   - indexName: An index needs a unique name to identify it
	///   - table: The associated table
	///   - fields: An array of columns
	public func assertIndex(indexName: String, table: String, fields: [String]) {
		if indexExists(indexName) {
			return
		}
		var sql = "CREATE INDEX \(indexName) ON [\(table)] (["
		sql += fields.toDelimitedString(delimiter: "],[")
		sql += "])"
		assertIndex(indexName: indexName, createSql: sql)
	}
	
	public func assertColumn(tableName: String, nameAndTypes: [String:String]) {
		
		//The table might not exist
		if !tableExists(tableName) {
			//We need to create it!!
			var sql = "CREATE TABLE [\(tableName)] ("
			var first = true
			for name in nameAndTypes.keys {
				if !first {
					sql += ", "
				}
				else {
					first = false
				}
				sql += "[\(name)] " + nameAndTypes[name]!
			}
			sql += ")"
			execute(sql)
			return
		}
		
		//But if it does we need to maybe add columns to it
		var cols : [String:String] = getColumnDetailsForTable(tableName: tableName)
		for name in nameAndTypes.keys {
			let lcName = name.lowercased()
			if !cols.keys.contains(lcName) {
				self.execute("ALTER TABLE [\(tableName)] ADD COLUMN [\(name)] " + nameAndTypes[name]!)
				cols[lcName] = nameAndTypes[name]
			}
		}
	}
	
	public func getColumnDetailsForTable(tableName: String) -> [String:String] {
		let _db = openDB()
		if _db == nil {
			return [:]
		}
		defer {
			closeDB(_db)
		}
		
		var ret : [String:String] = [:]
		var statement: SQLitePointer? = nil
		if sqlite3_prepare(_db, "PRAGMA table_info(\(tableName))", -1, &statement, nil) == SQLITE_OK {
			var step = sqlite3_step(statement)
			while step != SQLITE_DONE {
				if step == SQLITE_ROW {
					var col = "", val = ""
					for c in 0..<sqlite3_column_count(statement) {
						if String(cString: sqlite3_column_name(statement, c)).lowercased() == "name" {
							col = String(cString: sqlite3_column_text(statement, c)).lowercased()
						}
						if String(cString: sqlite3_column_name(statement, c)).lowercased() == "type" {
							val = String(cString: sqlite3_column_text(statement, c)).uppercased()
						}
					}
					if col != "" && val != "" {
						ret[col] = val
					}
				}
				step = sqlite3_step(statement)
			}
		}
		else {
			let errmsg = String(cString: sqlite3_errmsg(_db))
			print("E: " + errmsg)
		}
		sqlite3_finalize(statement)
		return ret
	}
	
	public func newRow(tableName: String) -> SQLRow {
		let rows = queryMultiRow("SELECT * FROM [\(tableName)] LIMIT 0")
		guard rows.count > 0 else {
			//If the table is valid we should never get here
			let cnat = getColumnDetailsForTable(tableName: tableName)
			return SQLRow(columnDefinitions: cnat)
		}
		return rows[0]
	}
	
	
	public func queryRowsAsJson(_ sql: String, _ parms: Any?...) -> String {
		let _db = openDB()
		if _db == nil {
			return ""
		}
		defer {
			closeDB(_db)
		}
		var statement : SQLitePointer? = nil
		var ret = "\"rows\": ["
		var rowCount = 0
		if sqlite3_prepare(_db, sql, -1, &statement, nil) == SQLITE_OK {
			bindParameters(statement: statement, parameters: parms)
			var step = sqlite3_step(statement)
			while step != SQLITE_DONE {
				if step == SQLITE_ROW {
					let colCount = sqlite3_column_count(statement)
					if rowCount > 0 {
						ret += ", "
					}
					rowCount += 1
					
					var items: [Any?] = []
					for i in 0..<colCount {
						items.append(String(cString: sqlite3_column_name(statement, i)))
						if statement.isNull(index: i)
						{
							items.append(nil)
						}
						else {
							items.append(String(cString: sqlite3_column_text(statement, i)))
						}
					}
					ret += Meta.toJson(items)
				}
				step = sqlite3_step(statement)
			}
		}
		else {
			let errmsg = String(cString: sqlite3_errmsg(_db))
			print("E: " + errmsg)
		}
		ret += "] }"
		ret = "{ \"rowcount\": \(rowCount), " + ret
		sqlite3_finalize(statement)
		return ret.replacingOccurrences(of: "\\\"", with: "\"").replacingOccurrences(of: "\\\\", with: "\\")
	}
	
	public func querySingleRow(_ sql: String, _ parms: Any?...) -> SQLRow {
		let ret = queryMultiRow(sql, parms)
		if ret.count > 0 {
			return ret[0]
		}
		return SQLRow()
	}
	
	public func queryMultiRow(_ sql: String, _ parms: Any?...) -> [SQLRow] {
		let _db = openDB()
		if _db == nil {
			return []
		}
		defer {
			closeDB(_db)
		}

		var ret: [SQLRow] = []
		var statement : SQLitePointer? = nil
		
		if sqlite3_prepare(_db, sql, -1, &statement, nil) == SQLITE_OK {
			bindParameters(statement: statement, parameters: parms)
			var step = sqlite3_step(statement)
			if step == SQLITE_DONE {
				//Just collect the columns
				ret.append(SQLRow(sqlData: statement, columnsOnly: true))
			}
			while step != SQLITE_DONE {
				if step == SQLITE_ROW {
					ret.append(SQLRow(sqlData: statement))
				}
				step = sqlite3_step(statement)
			}
		}
		else {
			let errmsg = String(cString: sqlite3_errmsg(_db))
			print("E: " + errmsg)
		}

		sqlite3_finalize(statement)
		return ret
		
	}
	
	public func collectColumnDataDelimited<T>(_ sql: String, column: String, hintType: T, delimiter: String = ",", _ parms: Any?...) -> String {
		return collectColumnData(sql, column: column,  hintType: hintType, parms).toDelimitedString(delimiter: delimiter)
	}
	
	public func collectColumnData<T>(_ sql: String, column: String = "", hintType: T, _ parms: Any?...) -> [T] {
		let _db = openDB()
		if _db == nil {
			return []
		}
		defer {
			closeDB(_db)
		}

		var ret: [T] = []
		var statement : SQLitePointer? = nil
		
		if sqlite3_prepare(_db, sql, -1, &statement, nil) == SQLITE_OK {
			bindParameters(statement: statement, parameters: parms)
			var step = sqlite3_step(statement)
			let index = columnIndex(ptr: statement, columnName: column)
			
			while step != SQLITE_DONE {
				if step == SQLITE_ROW {
					if hintType is Int {
						ret.append(sqlite3_column_int(statement, index) as! T)
					} else {
						if hintType is String {
							ret.append(String(cString: sqlite3_column_text(statement, index)) as! T)
						} else {
							if hintType is Int64 {
								ret.append(sqlite3_column_int64(statement, index) as! T)
							}
						}
					}
				}
				step = sqlite3_step(statement)
			}
		}
		else {
			let errmsg = String(cString: sqlite3_errmsg(_db))
			print("E: " + errmsg)
		}

		sqlite3_finalize(statement)
		return ret
	}
	
	private func columnIndex(ptr: SQLitePointer?, columnName: String) -> Int32 {
		if ptr == nil {
			return -1
		}
		let count = sqlite3_column_count(ptr)
		for i in 0..<count {
			if columnName.caseInsensitiveCompare(String(cString: sqlite3_column_name(ptr, i))) == ComparisonResult.orderedSame {
				return i
			}
		}
		return -1
	}
	
	//TODO - We collect the data into the SQLRow class. We are changing this so we can alter the data in the row. We
	//need to be able to write the data back to SQLite in some way from the SQLRow. We may need to record whether it has
	//changed or not.
	
	public func updateTableFromSQLRow(row: SQLRow, sourceTable: String, idColumn: CodingKey, updateDelegate: SqliteUpdateDelegate?) -> Bool {
		return updateTableFromSQLRow(row: row, sourceTable: sourceTable, idColumn: idColumn.stringValue, updateDelegate: updateDelegate)
	}
	public func updateTableFromSQLRow(row: SQLRow, sourceTable: String, idColumn: String, updateDelegate: SqliteUpdateDelegate? = nil) -> Bool {
		let _db = openDB()
		if _db == nil {
			return false
		}
		defer {
			closeDB(_db)
		}

		assert(idColumn.trim().length > 0, "No id column has been provided.")
		assert(sourceTable.trim().length > 0, "No source table has been provided.")
		
		//No fancy-schmamcy column checking for me. Assume I wrote this code and know what I'm doing
		//If the identity column is blank then we can assume this is an INSERT, otherwise an UPDATE
		var isInsert = row.isNull(idColumn)
		if !isInsert {
			//Double-check by seeing if the record exists!!
			let selSql = "SELECT COUNT(*) FROM [\(sourceTable)] WHERE [\(idColumn)] = \(row.get(idColumn, -1))"
			isInsert = queryValue(selSql, 0) < 1
		}
		
		//INSERT INTO [Table] ([c0], [c1], ...) VALUES (?,?,?,?,?)
		//UPDATE [Table] SET [c0] = ., [c1] = ., ... WHERE [idColumn] = #
		var sql = "INSERT INTO [\(sourceTable)] ({columns}) VALUES ({values})"
		let cols = row.columns(ignoringColumns: idColumn)
		if isInsert {
			let columns = cols.joined(separator: ", ")
			var values = String.init(repeating: "?, ", count: cols.count)
			values = values.left(values.length - 2)
			sql = sql.replacingOccurrences(of: "{columns}", with: columns)
			sql = sql.replacingOccurrences(of: "{values}", with: values)
		}
		else {
			//We'll just assume that idColumn points to a string type. We're not enclosing it in quotes, so it should be ok.
			sql = "UPDATE [\(sourceTable)] SET {columnEqualsValue} WHERE [\(idColumn)] = \(row.get(idColumn, -1))"
			var cev : [String] = []
			for col in cols {
				cev.append("[\(col)] = ?")
			}
			sql = sql.replacingOccurrences(of: "{columnEqualsValue}", with: cev.joined(separator: ", "))
		}
		
		var statement : SQLitePointer? = nil
		
		var ret = false
		
		if sqlite3_prepare_v2(_db, sql, -1, &statement, nil) == SQLITE_OK {
			
			var index = 1
			for col in cols {
				if statement.bindValue(index, value: row.get(col, "")) {
					index += 1
				}
				else {
					let err = sqlite3_errmsg(statement)
					print("\(String(describing: err))")
				}
			}
			if sqlite3_step(statement) == SQLITE_DONE {
				ret = true
			}
		}
		else {
			let errmsg = String(cString: sqlite3_errmsg(_db))
			print("E: " + errmsg)
		}
		sqlite3_finalize(statement)
		if isInsert {
			row.set(idColumn, sqlite3_last_insert_rowid(statement))
		}
		
		//Do we need to inform anything of our little updates?
		if let del = updateDelegate {
			if isInsert {
				del.RowHasUpdated(row: row)
			}
			else {
				del.RowAdded(row: row)
			}
		}
		return ret
	}
	
	private func unwrapParameters(parameters: [Any?]) -> [Any?] {
		var ret : [Any?] = []
		for item in parameters {
			if let moreItems = item as? [Any?] {
				ret.append(contentsOf: unwrapParameters(parameters: moreItems))
			}
			else {
				ret.append(item)
			}
		}
		return ret
	}
	
	private func bindParameters(statement: SQLitePointer?, parameters: [Any?]) {
		let parms = unwrapParameters(parameters: parameters)
		var index = 1
		for parm in parms {
			if statement.bindValue(index, value: parm) {
				index += 1
			}
			else {
				let err = sqlite3_errmsg(statement)
				print("\(String(describing: err))")
			}
		}
	}
	
	@discardableResult public func execute(_ sql: String, parms:Any?...) -> Int {
		let _db = openDB()
		if _db == nil {
			return -1
		}
		defer {
			closeDB(_db)
		}

		print(sql)
		
		var statement : SQLitePointer? = nil
		
		//var ret = false
		
		if sqlite3_prepare_v2(_db, sql, -1, &statement, nil) == SQLITE_OK {
			
			var index = 1
			for parm in parms {
				if statement.bindValue(index, value: parm) {
					index += 1
				}
				else {
					let err = sqlite3_errmsg(statement)
					print("\(String(describing: err))")
				}
			}
//			if sqlite3_step(statement) == SQLITE_DONE {
//				ret = true
//			}
		}
		else {
			let errmsg = String(cString: sqlite3_errmsg(_db))
			print("E: " + errmsg)
		}
		sqlite3_finalize(statement)
		let toRet = lastInsertedRowID(_db)

		return toRet
	}
	
	/// Instead of taking up a whole load of memory with an array, you can process
	/// the data row-by-row using a closure lambda function. Note the SQLRow is reused and if you want to keep a copy, use the clone() function of the SQLRow.
	/// - Parameters:
	///   - rowHandler: closure to handle the row of data
	///   - sql: SQL to collect the data
	///   - parms: parameters associated with the SQL
	@discardableResult
	public func multiRow(rowHandler: (SQLRow) -> Void, _ sql: String, _ parms: Any?...) -> Bool {

		let _db = openDB()
		if _db == nil {
			return false
		}
		defer {
			closeDB(_db)
		}

		let data = SQLRow()
		
		var statement : SQLitePointer? = nil
		
		if sqlite3_prepare(_db, sql, -1, &statement, nil) == SQLITE_OK {
			bindParameters(statement: statement, parameters: parms)
			var step = sqlite3_step(statement)
			if step == SQLITE_DONE {
				data.loadFromData(sqlData: statement, columnsOnly: true)
				rowHandler(data)
			}
			while step != SQLITE_DONE {
				if step == SQLITE_ROW {
					data.loadFromData(sqlData: statement, columnsOnly: false)
					rowHandler(data)
				}
				step = sqlite3_step(statement)
			}
		}
		else {
			let errmsg = String(cString: sqlite3_errmsg(_db))
			print("E: " + errmsg)
		}
		
		sqlite3_finalize(statement)
		return true
	}

	
	/// Use this for bulk operations
	/// - Parameters:
	///   - sql: Executable SQL with ? parameters
	///   - data: Bulk data
	@discardableResult public func bulkTransaction(_ sql: String, _ data : BulkData) -> Bool {
		let _db = openDB()
		if _db == nil {
			return false
		}
		defer {
			closeDB(_db)
		}

		
		var statement : SQLitePointer? = nil
		
		var ret = false
		
		execute("BEGIN IMMEDIATE TRANSACTION")
		if sqlite3_prepare_v2(_db, sql, -1, &statement, nil) == SQLITE_OK {

			for rowData in data.AllData {
				var index = 1
				for parm in rowData {
					if statement.bindValue(index, value: parm) {
						index += 1
					}
					else {
						let err = sqlite3_errmsg(statement)
						print("\(String(describing: err))")
					}
				}
				if sqlite3_step(statement) == SQLITE_DONE {
					ret = true
				}
				if sqlite3_reset(statement) == SQLITE_OK {
					continue
				}
				else {
					break
				}
			}
		}
		else {
			let errmsg = String(cString: sqlite3_errmsg(_db))
			print("E: " + errmsg)
		}
		sqlite3_finalize(statement)
		execute("COMMIT TRANSACTION")
		return ret
	}
}

