# Multi-stage: `docker build .` needs nothing on the host but Docker.
FROM eclipse-temurin:17.0.13_11-jdk-jammy AS build
WORKDIR /src
COPY gradlew ./
COPY gradle gradle
# Warm the Gradle distribution into its own layer so source edits don't re-download it.
RUN --mount=type=cache,target=/root/.gradle ./gradlew --version
COPY settings.gradle.kts build.gradle.kts ./
COPY common common
COPY analysis analysis
COPY format format
COPY indexer indexer
COPY proxy proxy
COPY watch watch
COPY app app
RUN --mount=type=cache,target=/root/.gradle ./gradlew --no-daemon installDist

FROM eclipse-temurin:17.0.13_11-jre-jammy

RUN useradd --system --uid 10001 kahshe
COPY --from=build /src/app/build/install/kahshe /opt/kahshe
USER 10001
EXPOSE 8282 8283

ENV KAHSHE_PORT=8282 \
    KAHSHE_ADMIN_PORT=8283 \
    JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"

HEALTHCHECK --interval=30s --timeout=3s CMD ["/bin/sh", "-c", \
  "wget -qO- http://127.0.0.1:8283/healthz || exit 1"]
ENTRYPOINT ["/opt/kahshe/bin/kahshe"]
