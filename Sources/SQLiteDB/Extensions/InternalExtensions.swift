//
//  InternalExtensions.swift
//  SQLDBInstance
//
//  Created by Matt Hogg on 07/02/2020.
//  Copyright © 2020 Matthew Hogg. All rights reserved.
//

import Foundation

extension String {
	func implies(_ doesItLookLikeThis: String...) -> Bool {
		return doesItLookLikeThis.first { (s) -> Bool in
			return self.caseInsensitiveCompare(s) == .orderedSame
		} != nil
	}
	
//	func encrypt(password: String, salt: Data, iv: Data) -> String {
//		do {
//			let digest = self.data(using: .utf8)
//			let key = try AES256.createKey(password: password.data(using: .utf8)!, salt: salt)
//			let aes = try AES256(key: key, iv: iv)
//			let encrypted = try aes.encrypt(digest!)
//			return encrypted.hexString
//		}
//		catch {
//			return ""
//		}
//	}
	
	func hexData() -> Data? {
		var data = Data(capacity: self.length / 2)
		
		let regex = try! NSRegularExpression(pattern: "[0-9a-f]{1,2}", options: .caseInsensitive)
		regex.enumerateMatches(in: self, range: NSRange(startIndex..., in: self)) { match, _, _ in
			let byteString = (self as NSString).substring(with: match!.range)
			let num = UInt8(byteString, radix: 16)!
			data.append(num)
		}
		
		guard data.count > 0 else { return nil }
		
		return data
	}
	
	var length : Int {
		return self.lengthOfBytes(using: .utf8)
	}
	
	func substring(from: Int, to: Int) -> String {
		guard from < self.length else { return "" }
		guard to >= 0 && from >= 0 && from <= to else { return "" }
		
		let start = index(startIndex, offsetBy: from)
		let end = index(startIndex, offsetBy: to.min(self.length - 1))
		return String(self[start...end])
	}
	
	func substring(from: Int, length: Int) -> String {
		let to = from - 1 + length
		return self.substring(from: from, to: to)
	}
	
	func substring(from: Int) -> String {
		let start = index(startIndex, offsetBy: from)
		let end = self.endIndex
		return String(self[start..<end])
	}
	
	func left(_ maxLen: Int) -> String {
		return self.substring(from: 0, to: maxLen - 1)
	}
}

extension Array where Element == Optional<Any> {
	func toDelimitedString(delimiter: String) -> String {
		return self.map { (e) -> String in
			return "\(e!)"
		}.joined(separator: delimiter)
	}
}

extension Array {
	func toDelimitedString(delimiter: String) -> String {
		return self.map { (e) -> String in
			return "\(e)"
		}.joined(separator: delimiter)
	}
}

extension Data {
	var hexString: String {
		return map { String(format:"%02hhx", $0)}.joined()
	}
}

extension Date {
	func toISOString() -> String {
		return ISO8601DateFormatter().string(from: self)
	}
	
	@discardableResult
	static func fromISOString(date: String) -> Date {
		return ISO8601DateFormatter().date(from: date) ?? Date()
	}
}

extension Comparable {
	func min(_ subsequent: Self...) -> Self {
		let minFromList = subsequent.min { (a, b) -> Bool in
			return a < b
		} ?? self
		return self < minFromList ? self : minFromList
	}
	
	func max(_ subsequent: Self...) -> Self {
		let maxFromList = subsequent.max { (a, b) -> Bool in
			return a > b
		} ?? self
		return self > maxFromList ? self : maxFromList
	}

	func isOneOf(_ inThis: Self...) -> Bool {
		return inThis.first { (c) -> Bool in
			return c == self
		} != nil
	}
}
