# Changelog

Formát vychází z [Keep a Changelog](https://keepachangelog.com/cs/1.1.0/), verze dodržují [SemVer](https://semver.org/lang/cs/).
Verze v `pom.xml` a `Chart.yaml` se musí rovnat tagu `vX.Y.Z` (viz kapitola 16 v [README](README.md)).

## [Unreleased]

## [1.1.1] - 2026-10-26

### Opraveno
- Image: adresáře `/etc/kafka-schema-guardian`, `config` a `rules` měly mód `0444` (bez práva `x`), protože `COPY --chmod=0444`
  nastavil mód i automaticky vytvořeným nadřazeným adresářům. Proxy (uid 185) se nedostala k namountovanému
  `proxy-config.yaml` a skončila hláškou `Given configuration file does not exist`. Adresáře se nyní vytvářejí
  výslovně s módem `0755`.

### Přidáno
- Kontrola při buildu image: uživatel proxy musí mít přístup do adresářů s konfigurací a číst soubory.
- Smoke test v pipeline před Trivy a publikací: kontejner spuštěný jako výchozí uživatel čte konfiguraci,
  včetně souboru namountovaného přes `-v`.

## [1.1.0] - 2026-10-26

### Změněno
- Jen nové sestavení a zvýšení verze (`pom.xml`, `Chart.yaml`, `Dockerfile`, `Makefile`, `deploy/base`, příklady v README).
  Funkce jsou stejné jako v 1.0.0.

### Známé problémy
- Image neobsahuje čitelnou konfiguraci pro uživatele proxy (adresáře `0444`), proxy v Kubernetes nenastartuje.
  **Nepoužívejte, opraveno v 1.1.1.**

## [1.0.0] - 2026-10-26

První vydání.

### Přidáno
- Kroxylicious filtr `SchemaGuardian`: kontroluje každý `ProduceRequest` podle magic byte a schema ID (Confluent wire
  format) proti whitelistu `topic -> povolená schema ID`; nevyhovující request se odmítne atomicky chybou
  `INVALID_RECORD` a broker o něm nic nezjistí. Při `acks=0` se request zahodí.
- Fail-closed chování: chybná konfigurace, výjimka při validaci nebo neočekávaná struktura požadavku zprávu nikdy
  nepropustí.
- `defaultPolicy` pro topicy, které nejsou ve whitelistu: `ALLOW` (guardian si jich nevšímá, nic se nečte ani
  nedekomprimuje) nebo `DENY` (zápis se odmítne). Hodnota je povinná.
- Prometheus metriky `kafka_schema_guardian_*` a strukturované logování odmítnutí s omezením četnosti.
- Image postavený na oficiálním `quay.io/kroxylicious/proxy:0.25.1` (pinnutý digestem), běží jako non-root uid 185.
- Kubernetes: manifesty s Kustomize (`deploy/`), Helm chart `kafka-schema-guardian-chart`, volitelný ServiceMonitor a HPA.
- `Makefile` pro build, testy, image (Docker, Podman, Buildah), nasazení a Helm.
- Pipeline GitHub Actions: build a testy, kontrola manifestů, Trivy sken, SBOM, publikace multi-arch image
  (`linux/amd64`, `linux/arm64`) a Helm chartu (OCI) na Docker Hub.
- Logo a banner v README (`docs/`).

### Známé problémy
- Image neobsahuje čitelnou konfiguraci pro uživatele proxy, viz 1.1.0. **Opraveno v 1.1.1.**

[Unreleased]: https://github.com/janbracha/kafka-guardian/compare/v1.1.1...HEAD
[1.1.1]: https://github.com/janbracha/kafka-guardian/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/janbracha/kafka-guardian/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/janbracha/kafka-guardian/releases/tag/v1.0.0
