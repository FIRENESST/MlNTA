// swift-tools-version:5.7
// Swift binding to libbeep.
// Build: swift build -Xlinker -L/path/to/libbeep    Test: .build/debug/beep-test 127.0.0.1:10288
import PackageDescription

let package = Package(
    name: "Beep",
    products: [.library(name: "Beep", targets: ["Beep"])],
    targets: [
        .systemLibrary(name: "CBeep", path: "Sources/CBeep"),
        .target(name: "Beep", dependencies: ["CBeep"]),
        .executableTarget(name: "beep-test", dependencies: ["Beep"]),
    ]
)
