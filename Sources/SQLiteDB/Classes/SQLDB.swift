//
//  SQLDB.swift
//  DBLib
//
//  Created by Matt Hogg on 06/06/2019.
//  Copyright © 2019 Matthew Hogg. All rights reserved.
//

import Foundation
import SQLite3
import LoggingFramework

internal class SQLDB : BaseIndentLog {
	
	public typealias SQLitePointer = OpaquePointer
	
	static let shared = SQLDB()
	
	// Initialization
	
	private override init() {
		super.init()
	}
	
	internal static var _db : SQLitePointer? = nil
	
	
	public static func tableExists(_ name: String) -> Bool {
		let sql = "SELECT count(*) FROM sqlite_master WHERE type='table' AND name LIKE ?;"
		let result = queryValue(sql, 0, name)
		return result > 0
	}
	
	public static func indexExists(_ name: String) -> Bool {
		let table = name.replacingOccurrences(of: "[", with: "").replacingOccurrences(of: "]", with: "").trim()
		let sql = "SELECT count(*) FROM sqlite_master WHERE type='index' AND name LIKE ?;"
		let result = queryValue(sql, "", table)
		if !result.isOneOf("0", "") {
			return true
		}
		return false
	}
	
	public static var assertDB : Bool {
		get {
			if _db == nil {
				open()
			}
			return _db != nil
		}
	}
	
	public static func open(path: URL, openCurrent: Bool = false) {
		open(path: path.path, openCurrent: openCurrent)
	}

	public static func open(openCurrent : Bool = false) {
		open(path: "", openCurrent: openCurrent)
	}
	public static func open(path: String, openCurrent : Bool = false) {
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
				try fm.copyItem(atPath: path, toPath: path + " [\(backup)]")
				try fm.removeItem(atPath: path)
			}
			catch {
				
			}
		}
		
		if sqlite3_open(sqlPath, &_db) == SQLITE_OK {
			return
		}
		return
	}
	
	public static func close() {
		if let db = _db {
			sqlite3_close(db)
		}
		_db = nil
	}
	
	public static func queryValue<T>(_ sql: String, _ defaultValue: T, _ parms: Any?...) -> T {
		if !assertDB {
			return defaultValue
		}
		var statement : SQLitePointer? = nil
		var ret = defaultValue
		if sqlite3_prepare(_db, sql, -1, &statement, nil) == SQLITE_OK {
			bindParameters(statement: statement, parms: parms)
			
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
		sqlite3_finalize(statement)
		return ret
	}
	
	public static func assertIndex(indexName: String, createSql: String) {
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
	public static func assertIndex(indexName: String, table: String, fields: [String]) {
		if indexExists(indexName) {
			return
		}
		var sql = "CREATE INDEX \(indexName) ON [\(table)] (["
		sql += fields.toDelimitedString(delimiter: "],[")
		sql += "])"
		assertIndex(indexName: indexName, createSql: sql)
	}
	
	public static func assertColumn(tableName: String, nameAndTypes: [String:String]) {
		
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
			SQLDB.execute(sql)
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
	
	public static func getColumnDetailsForTable(tableName: String) -> [String:String] {
		if !assertDB {
			return [:]
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
		sqlite3_finalize(statement)
		return ret
	}
	
	public static func newRow(tableName: String) -> SQLRow {
		let rows = queryMultiRow("SELECT * FROM [\(tableName)] LIMIT 0")
		guard rows.count > 0 else {
			//If the table is valid we should never get here
			let cnat = getColumnDetailsForTable(tableName: tableName)
			return SQLRow(columnDefinitions: cnat)
		}
		return rows[0]
	}
	
	
	public static func queryRowsAsJson(_ sql: String, _ parms: Any?...) -> String {
		if !assertDB {
			return ""
		}
		var statement : SQLitePointer? = nil
		var ret = "\"rows\": ["
		var rowCount = 0
		if sqlite3_prepare(_db, sql, -1, &statement, nil) == SQLITE_OK {
			bindParameters(statement: statement, parms: parms)
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
		ret += "] }"
		ret = "{ \"rowcount\": \(rowCount), " + ret
		sqlite3_finalize(statement)
		return ret.replacingOccurrences(of: "\\\"", with: "\"").replacingOccurrences(of: "\\\\", with: "\\")
	}
	
	public static func querySingleRow(_ sql: String, _ parms: Any?...) -> SQLRow {
		let ret = queryMultiRow(sql, parms)
		if ret.count > 0 {
			return ret[0]
		}
		return SQLRow()
	}
	
	public static func queryMultiRow(_ sql: String, _ parms: Any?...) -> [SQLRow] {
		if !assertDB {
			return []
		}
		var ret: [SQLRow] = []
		var statement : SQLitePointer? = nil
		
		if sqlite3_prepare(_db, sql, -1, &statement, nil) == SQLITE_OK {
			bindParameters(statement: statement, parms: parms)
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
		
		sqlite3_finalize(statement)
		return ret
		
	}
	
	/// Instead of taking up a whole load of memory with an array, you can process
	/// the data row-by-row using a closure lambda function.
	/// - Parameters:
	///   - rowHandler: closure to handle the row of data
	///   - sql: SQL to collect the data
	///   - parms: parameters associated with the SQL
	public static func processMultiRow(rowHandler: (SQLRow) -> Void, _ sql: String, _ parms: Any?...) -> Bool {
		if !assertDB {
			return false
		}
		let data = SQLRow()
		
		var statement : SQLitePointer? = nil
		
		if sqlite3_prepare(_db, sql, -1, &statement, nil) == SQLITE_OK {
			bindParameters(statement: statement, parms: parms)
			var step = sqlite3_step(statement)
			if step == SQLITE_DONE {
				data.loadFromData(sqlData: statement, columnsOnly: true)
				rowHandler(data)
			}
			while step != SQLITE_DONE {
				if step == SQLITE_ROW {
					data.loadFromData(sqlData: statement, columnsOnly: true)
					rowHandler(data)
				}
				step = sqlite3_step(statement)
			}
		}
		
		sqlite3_finalize(statement)
		return true
	}

	public static func collectColumnDataDelimited<T>(_ sql: String, column: String, hintType: T, delimiter: String = ",", _ parms: Any?...) -> String {
		return collectColumnData(sql, column: column,  hintType: hintType, parms).toDelimitedString(delimiter: delimiter)
	}
	
	public static func collectColumnData<T>(_ sql: String, column: String = "", hintType: T, _ parms: Any?...) -> [T] {
		if !assertDB {
			return []
		}
		var ret: [T] = []
		var statement : SQLitePointer? = nil
		
		if sqlite3_prepare(_db, sql, -1, &statement, nil) == SQLITE_OK {
			bindParameters(statement: statement, parms: parms)
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
		
		sqlite3_finalize(statement)
		return ret
	}
	
	private static func columnIndex(ptr: SQLitePointer?, columnName: String) -> Int32 {
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
	
	public static func updateTableFromSQLRow(row: SQLRow, sourceTable: String, idColumn: CodingKey, updateDelegate: SqliteUpdateDelegate?) -> Bool {
		return updateTableFromSQLRow(row: row, sourceTable: sourceTable, idColumn: idColumn.stringValue, updateDelegate: updateDelegate)
	}
	public static func updateTableFromSQLRow(row: SQLRow, sourceTable: String, idColumn: String, updateDelegate: SqliteUpdateDelegate? = nil) -> Bool {
		if !assertDB {
			return false
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
			var values = String(repeating: "?, ", count: cols.count)
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
	
	private static func unwrapParms(parms: [Any?]) -> [Any?] {
		var ret : [Any?] = []
		for item in parms {
			if let moreItems = item as? [Any?] {
				ret.append(contentsOf: unwrapParms(parms: moreItems))
			}
			else {
				ret.append(item)
			}
		}
		return ret
	}
	
	private static func bindParameters(statement: SQLitePointer?, parms: [Any?]) {
		let parms = unwrapParms(parms: parms)
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
	
	public static var DB : SQLitePointer? {
		get {
			return _db
		}
	}
	
	@discardableResult public static func execute(_ sql: String, parms:Any?...) -> Bool {
		if !assertDB {
			return false
		}
		print(sql)
		
		var statement : SQLitePointer? = nil
		
		var ret = false
		
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
			if sqlite3_step(statement) == SQLITE_DONE {
				ret = true
			}
		}
		sqlite3_finalize(statement)
		return ret
	}
	
	
	@discardableResult public static func bulkInsert(_ sql: String, parms: [Array<Any>]) -> Bool {
		if !assertDB {
			return false
		}
		
		var statement : SQLitePointer? = nil
		
		var ret = false
		
		execute("BEGIN IMMEDIATE TRANSACTION")
		if sqlite3_prepare_v2(_db, sql, -1, &statement, nil) == SQLITE_OK {

			for p in parms {
				if let ar = p as? Array<Any> {
					var index = 1
					for parm in ar {
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
				else {
					if !statement.bindValue(1, value: p) {
						let err = sqlite3_errmsg(statement)
						print("\(String(describing: err))")
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
		}
		sqlite3_finalize(statement)
		execute("COMMIT TRANSACTION")
		return ret
	}
}

public protocol SqliteUpdateDelegate : class {
	func RowHasUpdated(row: SQLRow)
	func RowAdded(row: SQLRow)
}

public extension Array where Element : StringProtocol {
	func sqlCSV() -> String {
		var ret : [String] = []
		for item in self {
			ret.append(item.replacingOccurrences(of: "'", with: "''"))
		}
		return "'" + ret.toDelimitedString(delimiter: "','") + "'"
	}
}

