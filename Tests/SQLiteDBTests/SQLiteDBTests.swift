import XCTest
@testable import SQLiteDB

final class SQLiteDBTests: XCTestCase {
    func testExample() {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct
        // results.
        XCTAssertEqual(SQLiteDB().text, "Hello, World!")
    }

    static var allTests = [
        ("testExample", testExample),
    ]
}
