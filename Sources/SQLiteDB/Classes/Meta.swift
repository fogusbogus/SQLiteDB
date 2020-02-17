//
//  Meta.swift
//  DBLib
//
//  Created by Matt Hogg on 06/06/2019.
//  Copyright © 2019 Matthew Hogg. All rights reserved.
//

import Foundation


/// Base class for the meta collection
open class Meta {
	
	private var _originalSignature = ""
	
	/// Initialise as empty
	public init() {
	}
	
	/// Initialise with some json from a string
	///
	/// - Parameter json: Json string
	public init(json: String) {
		load(json: json)
	}
	
	/// Reset the signature as an original state
	public func resetSignature() {
		_originalSignature = getSignature()
		for key in _coll.keys.filter({ (k) -> Bool in
			return _coll[k] is Meta
		}) {
			let meta = _coll[key] as! Meta
			meta.resetSignature()
		}
	}
	
	/// Does the key exist in the collection. Case-insensitive.
	///
	/// - Parameter key: The key to check
	/// - Returns: True if the key exists otherwise false
	public func hasKey(key: String) -> Bool {
		let ret = _coll.keys.first { (_id) -> Bool in
			return key.implies(_id)
		}
		return ret != nil
	}
	
	/// Load a collection from some Json string
	///
	/// - Parameters:
	///   - json: Json string
	///   - clear: Clear the current collection or merge?
	public func load(json: String, _ clear: Bool = true) {
		if clear {
			self.clear()
		}
		do {
			let parsedData = try JSONSerialization.jsonObject(with: json.data(using: .utf8)!, options: []) as! [String:Any]
			for key in parsedData.keys
			{
				let v = parsedData[key]
				if let dict = v as? Dictionary<String, Any> {
					let sub = Meta()
					sub.load(jsonDict: dict, false)
					self[key] = sub
				}
				else {
					if let vInt = v as? Int {
						self[key] = vInt
					}
					else {
						if let vBool = v as? Bool {
							self[key] = vBool
						}
						else {
							if let vString = v as? String {
								self[key] = vString
							}
							else {
								self[key] = v!
							}
						}
					}
				}
			}
		}
		catch let err {
			print(err)
		}
		_originalSignature = toJson(true)
	}
	
	/// Decrypt a string (AES256)
	///
	/// - Parameters:
	///   - key: Collection key name
	///   - password: Password to use to decrypt
	///   - salt: Salt
	///   - iv: IV
	/// - Returns: Decrypted string
	public func decrypt(key: String, password: String, salt: Data, iv: Data) -> String {
		return decrypt(value: get(key, ""), password: password, salt: salt, iv: iv)
	}
	/// Decrypt a string (AES256)
	///
	/// - Parameters:
	///   - value: String value to decrypt
	///   - password: Password to use to decrypt
	///   - salt: Salt
	///   - iv: IV
	/// - Returns: Decrypted string
	public func decrypt(value: String, password: String, salt: Data, iv: Data) -> String {
		return value.decrypt(password: password, salt: salt, iv: iv)
	}
	
	/// Encrypt a string (AES256)
	///
	/// - Parameters:
	///   - unencrypted: String to encrypt
	///   - password: Password to use to encrypt
	///   - salt: Salt
	///   - iv: IV
	/// - Returns: Encrypted string
	public func encrypt(unencrypted: String, password: String, salt: Data, iv: Data) -> String {
		return unencrypted.encrypt(password: password, salt: salt, iv: iv)
	}
	
	/// Has the data changed?
	///
	/// - Returns: True/false
	public func hasChanged() -> Bool {
		return _originalSignature != toJson(true)
	}
	
	private func load(jsonDict: Dictionary<String, Any>, _ clear: Bool = true) {
		if clear {
			self.clear()
		}
		for key in jsonDict.keys {
			let v = jsonDict[key]
			if let dict = v as? Dictionary<String, Any> {
				let sub = Meta()
				sub.load(jsonDict: dict, false)
				self[key] = sub
			}
			else {
				if let ary = v as? [Any] {
					let sub = Meta()
					sub.load(items: ary, false)
					self[key] = sub
				}
				else {
					if let vInt = v as? Int {
						self[key] = vInt
					}
					else {
						if let vBool = v as? Bool {
							self[key] = vBool
						}
						else {
							if let vString = v as? String {
								self[key] = vString
							}
							else {
								self[key] = v!
							}
						}
					}
				}
			}
		}
	}
	
	private func load(items: [Any], _ clear: Bool = true) {
		if clear {
			self.clear()
		}
		for item in items {
			if let dict = item as? Dictionary<String, Any> {
				load(jsonDict: dict, false)
			}
			else {
				if let ary = item as? [Any] {
					load(items: ary, false)
				}
			}
		}

	}
	
	private var _coll : [String:Any] = [:]
	
	/// Get/set a meta value
	///
	/// - Parameter key: String key value
	public subscript(key: String) -> Any {
		get {
			return self[key, nil]
		}
		set {
			self[key, nil] = newValue
		}
	}
	
	/// Get/set a meta value
	///
	/// - Parameters:
	///   - key: String key value
	///   - crypto: Encryption/decryption
	public subscript(key: String, _ crypto: MetaCryptoDelegate?) -> Any {
		get {
			let mKey = matchedKey(key: key)
			if let ret = _coll[mKey] {
				if ret is String {
					if crypto != nil {
						return crypto!.decrypt(ret as! String)
					}
				}
				return ret
			}
			return ""
		}
		set {
			let mKey = matchedKey(key: key)
			remove(key: mKey)
			if newValue is String {
				if crypto != nil {
					_coll[mKey] = crypto!.encrypt(newValue as! String)
					return
				}
			}
			_coll[mKey] = newValue
		}
	}
	
	public func setOrRemove<T>(_ key: String, _ value: T?, _ crypto: MetaCryptoDelegate? = nil) {
		if value == nil {
			remove(key: key)
			return
		}
		else {
			if value is String {
				if let v = value as? String {
					if v.length == 0 {
						remove(key: key)
						return
					}
				}
			}
			set(key, value!, crypto)
		}
	}
	
	public func set<T>(_ key: String, _ value: T, _ crypto: MetaCryptoDelegate? = nil) {
		self[key, crypto] = value
	}
	
	open func get<T>(_ key: String, _ defaultValue: T, _ crypto: MetaCryptoDelegate? = nil) -> T {
		if !hasKey(key: key) {
			return defaultValue
		}
		let ret = "\(self[key, crypto])"
		if defaultValue is String {
			return ret as? T ?? defaultValue
		}
		if defaultValue is Int {
			return Int(ret) as? T ?? defaultValue
		}
		if defaultValue is Bool {
			return Bool(ret) as? T ?? defaultValue
		}
		return defaultValue
	}
	
	public func add(collection: [String:Any], _ crypto: MetaCryptoDelegate? = nil) {
		for (k,v) in collection {
			self[k, crypto] = v
		}
	}
	
	@discardableResult
	public func addSub(key: String, _ json: String) -> Meta {
		let ret = Meta(json: json)
		self[key] = ret
		return ret
	}
	
	@discardableResult
	public func remove(key: String) -> Bool {
		let mKey = matchedKey(key: key)
		if _coll.contains(where: { (k, v) -> Bool in
			return k == mKey
		}) {
			_coll.removeValue(forKey: mKey)
			return true
		}
		return false
	}
	
	public func clear() {
		_coll.removeAll()
	}
	
	public func getSignature(_ archiveOriginalState: Bool = false, _ archiveKey: String = "previous") -> String {
		guard archiveKey.trim().length > 0 else {
			return ""
		}
		var ret = toJson(true)
		if archiveOriginalState && ret != _originalSignature {
			if let p1 = self[archiveKey] as? Meta {
				remove(key: archiveKey)
				addSub(key: archiveKey, _originalSignature)
				ret = toJson(true)
				remove(key: archiveKey)
				self[archiveKey] = p1
			}
			else {
				if !hasKey(key: archiveKey) {
					addSub(key: archiveKey, _originalSignature)
					ret = toJson(true)
					remove(key: archiveKey)
				}
			}
		}
		return ret
	}
	
	private func matchedKey(key: String) -> String {
		let ret = _coll.keys.first { (_key) -> Bool in
			return key.implies(_key)
		}
		return ret ?? key.lowercased()
	}
	
	public func getJson() -> String {
		do {
			let ret = try JSONSerialization.data(withJSONObject: _coll)
			if let string = String(data: ret, encoding: String.Encoding.utf8) {
				return string
			}
		}
		catch {
			
		}
		return ""
	}
	
	public func toJson(_ sorted: Bool = false) -> String {
		let dict = toDictionary()
		if sorted {
			do {
				let json = try JSONSerialization.data(withJSONObject: dict, options: .sortedKeys)
				if let string = String(data: json, encoding: String.Encoding.utf8) {
					return string
				}
			}
			catch {}
			return ""
		}
		else {
			do {
				let json = try JSONSerialization.data(withJSONObject: dict)
				if let string = String(data: json, encoding: String.Encoding.utf8) {
					return string
				}
			}
			catch {}
			return ""
		}
	}
	
	internal func toDictionary() -> Dictionary<String, Any> {
		var ret = Dictionary<String, Any>()
		for key in _coll.keys {
			if let v = _coll[key] as? Meta {
				ret[key] = v.toDictionary()
			}
			else {
				ret[key] = _coll[key]
			}
		}
		return ret
	}
	
	public static func toJson(_ args: Any?...) -> String {
		var dict: [String:Any] = [:]
		var isKey = true
		var key = ""
		for arg in args {
			if isKey {
				key = "\(arg!)"
			}
			else {
				if let a = arg {
					dict[key] = "\(a)"
				}
				else {
					dict[key] = "{NULL}"
				}
			}
			isKey = !isKey
		}
		do {
			let ret = try JSONSerialization.data(withJSONObject: dict)
			if let string = String(data: ret, encoding: String.Encoding.utf8) {
				return string
			}
			return ""
		}
		catch {
			print(error.localizedDescription)
			return ""
		}
	}
	
	public static func toJson(_ args: [Any?]) -> String {
		var dict: [String:Any] = [:]
		var isKey = true
		var key = ""
		for arg in args {
			if isKey {
				key = "\(arg!)"
			}
			else {
				if let a = arg {
					dict[key] = a
				}
				else {
					dict[key] = "{NULL}"
				}
			}
			isKey = !isKey
		}
		do {
			let ret = try JSONSerialization.data(withJSONObject: dict)
			if let string = String(data: ret, encoding: String.Encoding.utf8) {
				return string
			}
			return ""
		}
		catch {
			print(error.localizedDescription)
			return ""
		}
	}
}

public protocol MetaCryptoDelegate {
	func encrypt(_ value: String) -> String
	func decrypt(_ value: String) -> String
}
