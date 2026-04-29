namespace NRedisStack.Tests;

public static class ServerVersion
{    
    // we might be able to remove this in the future - there is a suggestion that there is a
    // formal policy to start milestones at 224 (so 8.7.224 is *effectively* 8.8.0-pre-alpha)
    public static Version Redis_8_8 = new(8, 7, 226);
}