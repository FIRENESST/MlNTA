// swift-tools-version:5.7
// Swift binding to libbeep. NOT verified locally (no Swift toolchain here).
// Build: swift build -Xlinker -L/path/to/libbeep
import PackageDescription

let package = Package(
    name: "Beep",
    products: [.library(name: "Beep", targets: ["Beep"])],
    targets: [
        .systemLibrary(name: "CBeep", path: "Sources/CBeep"),
        .target(name: "Beep", dependencies: ["CBeep"]),
    ]
)
