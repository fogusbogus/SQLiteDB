//
//  OpaquePointer.swift
//  DBLib
//
//  Created by Matt Hogg on 06/06/2019.
//  Copyright © 2019 Matthew Hogg. All rights reserved.
//

import Foundation
import SQLite3

extension Optional where Wrapped == OpaquePointer {
	func isNull(index: Int32) -> Bool {
		if let _ = sqlite3_column_text(self, index) {
			return false
		}
		else {
			return true
		}
	}
	
	func bindValue(_ index: Int, value: Any?) -> Bool {
		let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
		let i = Int32(index)
		if value == nil {
			sqlite3_bind_null(self, i)
		}
		else {
			if value is Int {
				let pValue = Int32((value as? Int)!)
				return sqlite3_bind_int(self, i, pValue) == SQLITE_OK
			}
			if value is Int64 {
				let pValue = Int64((value as? Int64)!)
				return sqlite3_bind_int64(self, i, pValue) == SQLITE_OK
			}
			if value is Double || value is Float {
				let pValue = Double((value as? Double)!)
				return sqlite3_bind_double(self, i, pValue) == SQLITE_OK
			}
			if value is Bool {
				let pValue = Bool((value as? Bool)!)
				if pValue {
					return sqlite3_bind_int(self, i, 1) == SQLITE_OK
				}
				return sqlite3_bind_int(self, i, 0) == SQLITE_OK
			}
			if value is Date {
				let pValue = (value as? Date)!
				let text = pValue.toISOString()
				return sqlite3_bind_text(self, i, text.toUTF8(), -1, SQLITE_TRANSIENT) == SQLITE_OK
			}
			if 1 == 1
			{
				let text : String = (value as? String)!
				let enc = text.cString(using: String.Encoding.utf8)!
				
				return sqlite3_bind_text(self, i, enc, -1, SQLITE_TRANSIENT) == SQLITE_OK
			}
			
		}
		return false
	}
}


extension String {
	
	func trim() -> String {
		return self.trimmingCharacters(in: .whitespaces)
	}
	
	func toUTF8() -> String {
		return String(utf8String: self.cString(using: .utf8)!)!
	}
	
	func toInt(_ defaultValue: Int = 0) -> Int {
		return Int(self) ?? defaultValue
	}
	func toInt64(_ defaultValue: Int64 = 0) -> Int64 {
		return Int64(self) ?? defaultValue
	}
	func toFloat(_ defaultValue: Float = 0) -> Float {
		return Float(self) ?? defaultValue
	}
	func toDouble(_ defaultValue: Double = 0.0) -> Double {
		return Double(self) ?? defaultValue
	}
	func toDecimal(_ defaultValue: Decimal = 0) -> Decimal {
		return Decimal(string: self) ?? defaultValue
	}
	func toBool(_ defaultValue: Bool = false) -> Bool {
		return self.substring(from: 0, to: 1).lowercased().isOneOf("t", "1", "y")
	}
	func toDate(_ defaultValue: Date) -> Date {
		return ISO8601DateFormatter().date(from: self) ?? defaultValue
	}
}

