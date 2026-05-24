FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY Shared/Shared.csproj Shared/
COPY TrackingService/TrackingService.csproj TrackingService/
RUN dotnet restore TrackingService/TrackingService.csproj

COPY Shared/ Shared/
COPY TrackingService/ TrackingService/
RUN dotnet publish TrackingService/TrackingService.csproj -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app

RUN adduser --disabled-password --gecos '' appuser && chown -R appuser /app
USER appuser

COPY --from=build /app/publish .

EXPOSE 8080
ENTRYPOINT ["dotnet", "TrackingService.dll"]
