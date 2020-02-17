import XCTest

import SQLiteDBTests

var tests = [XCTestCaseEntry]()
tests += SQLiteDBTests.allTests()
XCTMain(tests)
