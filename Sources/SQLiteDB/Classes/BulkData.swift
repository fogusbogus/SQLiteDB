//
//  BulkData.swift
//  DBLib
//
//  Created by Matt Hogg on 02/12/2019.
//  Copyright © 2019 Matthew Hogg. All rights reserved.
//

import Foundation

/// Use this to prepare bulk data for SQL operations
public class BulkData {
	
	public init() {
		
	}
	
	/*
	We keep an array of items which we push to via the current collection.
	*/
	private var _array : [[Any?]] = []
	private var _current : [Any?] = []
	
	/// Push the data - you need to do this for each row and before you call the bulk SQL operation
	public func pushRow() {
		_array.append(_current)
		_current = []
	}
	
	/// Add the next piece of data
	/// - Parameter value: Data to add
	public func add(_ value: Any?...) {
		for v in value {
			_current.append(v)
		}
	}
		
	public var AllData : [[Any?]] {
		get {
			return _array
		}
	}
	
	/// Clear the current data
	public func clear() {
		_current = []
		_array = []
	}
}
