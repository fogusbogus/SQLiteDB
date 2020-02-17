//
//  SQLRow.swift
//  DBLib
//
//  Created by Matt Hogg on 06/06/2019.
//  Copyright © 2019 Matthew Hogg. All rights reserved.
//

import Foundation

import SQLite3

public class SQLRow {
	
	public var RowChangedHandler : SQLRowChangedDelegate? = nil
	
	public typealias SQLitePointer = OpaquePointer
	
	public typealias Long = Int64
	private var _data : [String:Any?] = [:]
	private var _keyMap : [String:String] = [:]
	private var _signature : String = ""
	private var _isDirty = false
	
	public func clone() -> SQLRow {
		let clone = SQLRow()
		_data.keys.forEach { (key) in
			clone._data[key] = _data[key]
		}
		_keyMap.keys.forEach { (key) in
			clone._keyMap[key] = _keyMap[key]
		}
		clone._signature = _signature
		clone._isDirty = _isDirty
		return clone
	}
	
	private func getSignature() -> String {
		var ret = ""
		let keys = _keyMap.keys.sorted()
		for key in keys {
			ret += key + "\t"
			if _data[key] == nil {
				ret += "nil"
			}
			else {
				ret += "\"\(String(describing: _data[key]))\""
			}
		}
		return ret
	}
	
	public func columns(ignoringColumns: String...) -> [String] {
		var ret : [String] = []
		var lcCols : [String] = []
		for item in ignoringColumns {
			lcCols.append(item.lowercased())
		}
		
		for key in _keyMap.keys {
			if !lcCols.contains(key) {
				ret.append(key)
			}
		}
		
		return ret
	}
	
	var IsEmpty: Bool {
		return _data.count == 0
	}
	
	public init(columnDefinitions: [String:String]) {
		
	}
	
	public init() {
	}
	
	public init(sqlData: SQLitePointer?, columnsOnly: Bool = false) {
		loadFromData(sqlData: sqlData, columnsOnly: columnsOnly)
	}
	
	public func loadFromData(sqlData: SQLitePointer?, columnsOnly: Bool = false) {
		_data = [:]
		_keyMap = [:]
		_signature = ""
		_isDirty = false
		
		let colCount = sqlite3_column_count(sqlData)
		
		for i in 0..<colCount {
			let colName = String(cString: sqlite3_column_name(sqlData, i))
			if !columnsOnly {
				if sqlData.isNull(index: i)
				{
					_data[colName] = nil
				}
				else {
					switch sqlite3_column_type(sqlData, i) {
					case SQLITE_INTEGER:
						_data[colName] = sqlite3_column_int(sqlData, i)
						break
					case SQLITE_BLOB:
						_data[colName] = sqlite3_column_blob(sqlData, i)
						break
					case SQLITE_FLOAT:
						_data[colName] = sqlite3_column_double(sqlData, i)
						break
					case SQLITE_TEXT:
						let ptr = sqlite3_column_text(sqlData, i)
						_data[colName] = String(cString: ptr!)
					case SQLITE_NULL:
						_data[colName] = nil
						break
					default:
						break
					}
				}
			}
			else {
				_data[colName] = nil
			}
			_keyMap[colName.lowercased()] = colName
		}
		_signature = getSignature()
	}
	
	public var createNewKeys = false
	
	//We are not using case-sensitive keys. However, the collection does, so provide
	//a way to reference a key without worrying about the case.
	private func mapID(_ id: String) -> String {
		guard _keyMap[id.lowercased()] != nil else {
			if createNewKeys {
				_keyMap[id.lowercased()] = id
				return id
			}
			return id
		}
		return _keyMap[id.lowercased()]!
	}
	
	//Has the data been changed, or an attempt to change the data. To check for differences, use
	//Signature
	public var isDirty : Bool {
		get {
			return _isDirty //_signature != getSignature()
		}
	}
	
	public func signature(original: Bool = false) -> String {
		if original {
			return _signature
		}
		return getSignature()
	}
	
	public func resetDirty() {
		_isDirty = false
	}
	
	public func hasKey(_ id: String) -> Bool {
		guard _keyMap[id.lowercased()] != nil else {
			return false
		}
		return true
	}
	public func hasKey(_ id: CodingKey) -> Bool {
		return hasKey(id.stringValue)
	}
	
	public func set<T>(_ id: CodingKey, _ newValue: T?) {
		set(id.stringValue, newValue)
	}
	public func set<T>(_ id: String, _ newValue: T?) {
		if hasKey(id) {
			//Something might want to block the update
			if let rch = RowChangedHandler {
				if !rch.beforeValueChange(column: id, newValue: newValue) {
					return
				}
			}
			_data[mapID(id)] = newValue
			
			//I don't care if it's a different value or not I do care that I've tried
			//to set it
			_isDirty = true
			
			//Something might want to know a value is being set
			if let rch = RowChangedHandler {
				rch.afterValueChange(column: id, newValue: newValue)
			}
		}
	}
	
	subscript(id: String) -> String {
		get {
			return get(id, "")
		}
		set {
			set(id, newValue)
		}
	}
	subscript(key: CodingKey) -> String {
		get {
			return get(key, "")
		}
		set {
			set(key, newValue)
		}
	}
	
	subscript<T>(id: String, defaultValue: T) -> T {
		get {
			return get(id, defaultValue)
		}
		set {
			set(id, newValue)
		}
	}
	subscript<T>(key: CodingKey, defaultValue: T) -> T {
		get {
			return get(key, defaultValue)
		}
		set {
			set(key, newValue)
		}
	}
	
	public func get<T>(_ id: CodingKey, _ defaultValue: T) -> T {
		return get(id.stringValue, defaultValue)
	}
	public func get<T>(_ id: String, _ defaultValue: T) -> T {
		if hasKey(id) {
			if isNull(id) {
				return defaultValue
			}
			if defaultValue is Int {
				if let vInt = _data[mapID(id)] as? Int {
					return vInt as! T
				}
				if let v = _data[mapID(id)] as? Int32 {
					return Int(v) as! T
				}
				if let v64 = _data[mapID(id)] as? Int64 {
					return Int(v64) as! T
				}
				return defaultValue
			}
			if let ret = _data[mapID(id)] as? T {
				return ret
			}
		}
		return defaultValue
	}
	
	public func getNull<T>(_ id: CodingKey, _ hintValue: T) -> T? {
		return getNull(id.stringValue, hintValue)
	}
	public func getNull<T>( _ id: String, _ hintValue: T) -> T? {
		if hasKey(id) {
			if isNull(id) {
				return nil
			}
			if hintValue is Int {
				if let vInt = _data[mapID(id)] as? Int {
					return (vInt as! T)
				}
				if let v = _data[mapID(id)] as? Int32 {
					return (Int(v) as! T)
				}
				if let v64 = _data[mapID(id)] as? Int64 {
					return (Int(v64) as! T)
				}
				return hintValue
			}
			if let ret = _data[mapID(id)] as? T {
				return ret
			}
		}
		return nil
	}
	
	public func getNull<T>(_ index: Int, _ hintValue: T) -> T? {
		let id = Array(_keyMap.keys)[index]
		return getNull(id, hintValue)
	}
	
	public func get<T>(_ index: Int, _ defaultValue: T) -> T {
		let id = Array(_keyMap.keys)[index]
		return get(id, defaultValue)
	}
	
	public func columnIndex(_ id: String) -> Int? {
		return Array(_keyMap.keys).firstIndex(of: mapID(id))
	}
	
	public func text(_ id: String, _ defaultValue: String = "") -> String {
		if !hasKey(id) || isNull(id) {
			return defaultValue
		}
		let data = _data[mapID(id)]

		let rI = data as? Int
		if rI != nil {
			return "\(rI!)"
		}
		let r32 = data as? Int32
		if r32 != nil {
			return "\(r32!)"
		}
		let r64 = data as? Int64
		if r64 != nil {
			return "\(r64!)"
		}
		let rB = data as? Bool
		if rB != nil {
			return "\(rB!)"
		}
		let rD = data as? Date
		if rD != nil {
			return "\(rD!)"
		}
		let rT = data as? String
		if rT != nil {
			return "\(rT!)"
		}
		let r = "\(String(describing: data))"
		return r
		
	}
	
	//    private func getTryInt(id: String) throws -> Int {
	//
	//    }
	
	
	public func isNull(_ id: String) -> Bool {
		let mid = mapID(id)
		guard _data[mid] != nil else {
			return true
		}
		return _data[mid]! == nil
	}
	public func isNull(_ id: CodingKey) -> Bool {
		return isNull(id.stringValue)
	}
	
	public func toJsonString() -> String {
		if let ret = toJsonObject() {
			return String(data: ret, encoding: .utf8)!
		}
		return ""
	}
	
	private func toJsonObject() -> Data? {
		do {
			return try JSONSerialization.data(withJSONObject: _data, options: .prettyPrinted)
		}
		catch {
			return nil
		}
	}
}

public protocol SQLRowChangedDelegate : class {
	func beforeValueChange(column: String, newValue: Any?) -> Bool
	func afterValueChange(column: String, newValue: Any?)
}

public extension Array where Element == SQLRow {
	func columns() -> [String] {
		if self.count > 0 {
			return self[0].columns()
		}
		return []
	}
	
	func toJsonString() -> String {
		var ret = "{ rows: ["
		var first = true
		for row in self {
			if !first {
				ret += ", "
			}
			else {
				first = false
			}
			ret += row.toJsonString()
		}
		ret += "] }"
		return ret
	}
}
