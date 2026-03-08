namespace EngineeredWood.Orc.Tests;

internal static class TestHelpers
{
    private static readonly string TestDataDir = Path.Combine(
        AppContext.BaseDirectory, "..", "..", "..", "TestData");

    public static string GetTestFilePath(string fileName) =>
        Path.GetFullPath(Path.Combine(TestDataDir, fileName));
}
