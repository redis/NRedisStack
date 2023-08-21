# base development image on .NET 7.0
FROM mcr.microsoft.com/devcontainers/dotnet:0-7.0-bullseye AS build

# fetch the .NET 6.0 SDK for use in testing
COPY --from=mcr.microsoft.com/dotnet/sdk:6.0 /usr/share/dotnet/shared /usr/share/dotnet/shared