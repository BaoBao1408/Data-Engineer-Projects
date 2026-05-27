# Stage 1: Build
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY Shared/Shared.csproj Shared/
COPY TrackingService/TrackingService.csproj TrackingService/
RUN dotnet restore TrackingService/TrackingService.csproj

COPY Shared/ Shared/
COPY TrackingService/ TrackingService/
RUN dotnet publish TrackingService/TrackingService.csproj -c Release -o /app/publish

# Stage 2: Runtime
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app

# curl cho healthcheck — phải ở runtime stage, trước khi switch user
RUN apt-get update && apt-get install -y curl --no-install-recommends && rm -rf /var/lib/apt/lists/*

RUN adduser --disabled-password --gecos '' appuser && chown -R appuser /app
USER appuser

COPY --from=build /app/publish .

EXPOSE 8080
ENTRYPOINT ["dotnet", "TrackingService.dll"]