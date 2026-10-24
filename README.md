# kafka-schema-guardian

Kroxylicious filtr (Java 21), kterĂ˝ kontroluje **kaĹľdĂ˝ Kafka `ProduceRequest`** dĹ™Ă­v, neĹľ se dostane k brokeru.
Z kaĹľdĂ©ho recordu ÄŤte jen topic, dĂ©lku hodnoty, *magic byte* a *schema ID* podle Confluent wire formĂˇtu. Cokoli,
co nenĂ­ explicitnÄ› povoleno v whitelistu `topic -> povolenĂˇ schema ID`, se odmĂ­tne pĹ™Ă­mo v proxy a **broker o tom
poĹľadavku nikdy neuslyĹˇĂ­**.

* Ĺ˝ĂˇdnĂˇ deserializace Avro/Protobuf/JSON, ĹľĂˇdnĂ˝ Schema Registry lookup, ĹľĂˇdnĂ© kopĂ­rovĂˇnĂ­ ani logovĂˇnĂ­ payloadu.
* VĂ˝chozĂ­ politika `DENY`, fail-closed: chyba konfigurace, vĂ˝jimka pĹ™i validaci nebo neoÄŤekĂˇvanĂˇ struktura poĹľadavku
  nikdy neznamenĂˇ propuĹˇtÄ›nĂ­ zprĂˇvy.
* SamostatnĂ˝, bezstavovĂ˝ Kubernetes Deployment pĹ™ed existujĂ­cĂ­m Strimzi clusterem (ne sidecar).

## Obsah

1. [Verze a kompatibilita](#1-verze-a-kompatibilita)
2. [Architektura a datovĂ˝ tok](#2-architektura-a-datovĂ˝-tok)
3. [ValidaÄŤnĂ­ algoritmus](#3-validaÄŤnĂ­-algoritmus)
4. [ChovĂˇnĂ­ vĹŻÄŤi producerĹŻm](#4-chovĂˇnĂ­-vĹŻÄŤi-producerĹŻm)
5. [Konfigurace a whitelist](#5-konfigurace-a-whitelist)
6. [VĂ˝kon](#6-vĂ˝kon)
7. [LogovĂˇnĂ­ a metriky](#7-logovĂˇnĂ­-a-metriky)
8. [Build a testy](#8-build-a-testy)
9. [Docker image](#9-docker-image)
10. [LokĂˇlnĂ­ testovĂˇnĂ­](#10-lokĂˇlnĂ­-testovĂˇnĂ­)
11. [Kubernetes: Kustomize a Helm](#11-kubernetes-kustomize-a-helm)
12. [SĂ­tÄ›, Kafka metadata a Strimzi](#12-sĂ­tÄ›-kafka-metadata-a-strimzi)
13. [PLAINTEXT, TLS a mTLS](#13-plaintext-tls-a-mtls)
14. [ZmÄ›na whitelistu, rollback](#14-zmÄ›na-whitelistu-rollback)
15. [Dostupnost, ĹˇkĂˇlovĂˇnĂ­, nastavenĂ­ producerĹŻ](#15-dostupnost-ĹˇkĂˇlovĂˇnĂ­-nastavenĂ­-producerĹŻ)
16. [CI/CD](#16-cicd)
17. [Troubleshooting](#17-troubleshooting)
18. [BezpeÄŤnostnĂ­ omezenĂ­ a znĂˇmĂ© limitace](#18-bezpeÄŤnostnĂ­-omezenĂ­-a-znĂˇmĂ©-limitace)

---

## 1. Verze a kompatibilita

| SouÄŤĂˇst | Verze | PoznĂˇmka |
|---|---|---|
| Kroxylicious | **0.25.1** (vydĂˇno 2026-10-20) | nejnovÄ›jĹˇĂ­ stabilnĂ­ verze v Maven Central; API ovÄ›Ĺ™eno proti `kroxylicious-api-0.25.1` |
| Java | **21** | od Kroxylicious 0.22 je Java 21 minimum; testovĂˇno i na Java 25 upstreamem |
| Kafka message API | **4.3.x** | od 0.24 jsou tĹ™Ă­dy `*Data` vendorovanĂ© do `io.kroxylicious.kafka.*`; `kafka.message-spec.version` Kroxylicious 0.25.1 je 4.3.0 |
| Kafka klienti v testech | 4.3.1 | stejnĂˇ verze jako v Kroxylicious parent POM |
| Proxy image | `quay.io/kroxylicious/proxy:0.25.1` | pinnuto i digestem, viz [Dockerfile](Dockerfile) (`quay.io/kroxylicious/kroxylicious` je od 0.24 zruĹˇenĂ˝) |
| Strimzi | libovolnĂˇ | proxy se pĹ™ipojuje jako bÄ›ĹľnĂ˝ Kafka klient na TLS listener; fragmenty Strimzi CR v README jsou ilustrativnĂ­ |

### Pravidla kompatibility pluginu

1. **Plugin se kompiluje proti pĹ™esnĂ© verzi Kroxylicious a musĂ­ bÄ›Ĺľet na stejnĂ© verzi.** `kroxylicious.version`
   v [pom.xml](pom.xml) a tag `PROXY_IMAGE` v [Dockerfile](Dockerfile) se mÄ›nĂ­ vĹľdy spoleÄŤnÄ›.
2. Filter API se mezi minor verzemi lĂˇme (0.24: pĹ™echod na `io.kroxylicious.kafka.*`, `errorResponse(..., Errors)`
   mĂ­sto vĂ˝jimek; 0.22: ĹľivotnĂ­ cyklus `FilterFactory` per virtual cluster). Aktualizace = zmÄ›na verze, kompilace,
   unit testy a integraÄŤnĂ­ testy, teprve pak novĂ˝ image.
3. Plugin pouĹľĂ­vĂˇ z balĂ­ku `io.kroxylicious.kafka.common.record.internal` tĹ™Ă­dy `MemoryRecords`, `Record`,
   `RecordBatch`. Upstream tento balĂ­k oznaÄŤuje jako **nepodporovanĂ© API** (mĹŻĹľe se zmÄ›nit i v patch verzi). Je to ale
   jedinĂˇ cesta k dekomprimovanĂ˝m recordĹŻm a pouĹľĂ­vajĂ­ ji i vestavÄ›nĂ© filtry Kroxylicious (record validation, encryption).
   Proto pravidlo 1.
4. VĹˇechny zĂˇvislosti pluginu majĂ­ scope `provided`; JAR obsahuje jen vlastnĂ­ tĹ™Ă­dy (`jackson`, `micrometer`, `slf4j`
   dodĂˇvĂˇ proxy). Plugin se objevĂ­ pĹ™es `ServiceLoader`:
   [`META-INF/services/io.kroxylicious.proxy.filter.FilterFactory`](src/main/resources/META-INF/services/io.kroxylicious.proxy.filter.FilterFactory)
   obsahuje `cz.company.guardian.SchemaGuardian`; v konfiguraci se pouĹľĂ­vĂˇ jako `type: SchemaGuardian`.
5. Instance `FilterFactory` je od 0.22 vytvĂˇĹ™ena **pro kaĹľdĂ˝ virtual cluster zvlĂˇĹˇĹĄ**; whitelist se proto naÄŤĂ­tĂˇ
   per virtual cluster a metriky jsou sdĂ­lenĂ© pĹ™es globĂˇlnĂ­ Micrometer registry proxy.

---

## 2. Architektura a datovĂ˝ tok

```mermaid
flowchart LR
    P[Producer] -->|bootstrap + broker porty| S[Service kafka-schema-guardian-bootstrap]
    S --> D

    subgraph D[Deployment kafka-schema-guardian - 3+ replik]
        direction TB
        K[Kroxylicious proxy] --> F[SchemaGuardian filtr]
    end

    F -->|vĹˇe povoleno: ProduceRequest se pĹ™epoĹˇle| B[Strimzi Kafka bootstrap / brokery]
    F -.->|alespoĹ jeden record nevyhovuje: INVALID_RECORD, broker nic nedostane| P
```

OdmĂ­tnutĂ­:

```mermaid
sequenceDiagram
    participant P as Producer
    participant K as Kroxylicious
    participant G as SchemaGuardian
    participant B as Kafka broker
    P->>K: ProduceRequest
    K->>G: onProduceRequest
    G-->>K: errorResponse(INVALID_RECORD)
    K-->>P: ProduceResponse (INVALID_RECORD)
    Note over B: nic neobdrĹľĂ­
```

* Producer se pĹ™ipojuje na `kafka-schema-guardian-bootstrap`, ne na Strimzi bootstrap Service.
* Kroxylicious pĹ™episuje adresy brokerĹŻ v `Metadata` odpovÄ›dĂ­ch (`portIdentifiesNode`), takĹľe po bootstrapu klient
  mluvĂ­ opÄ›t s proxy, nikoli pĹ™Ă­mo s brokerem. Detaily v [kapitole 12](#12-sĂ­tÄ›-kafka-metadata-a-strimzi).
* Guardian je **prvnĂ­** filtr v `defaultFilters`: nic nesmĂ­ projĂ­t jinĂ˝m filtrem ani k brokeru, dokud ho neschvĂˇlĂ­.
* Replika neudrĹľuje ĹľĂˇdnĂ˝ stav kromÄ› naÄŤtenĂ©ho nemÄ›nnĂ©ho whitelistu. VĹˇechny repliky ÄŤtou stejnĂ˝ ConfigMap.

---

## 3. ValidaÄŤnĂ­ algoritmus

Implementace: [`RequestValidator`](src/main/java/cz/company/guardian/validation/RequestValidator.java) a
[`RecordChecker`](src/main/java/cz/company/guardian/validation/RecordChecker.java).

Pro kaĹľdĂ˝ record, v poĹ™adĂ­:

1. zjisti topic (Produce v13+ posĂ­lĂˇ jen `topicId`; jmĂ©no se pĹ™eloĹľĂ­ pĹ™es `FilterContext.topicNames`, coĹľ proxy cachuje),
2. topic nenĂ­ ve whitelistu -> `TOPIC_NOT_CONFIGURED`,
3. hodnota je `null` -> `NULL_VALUE`,
4. hodnota mĂˇ mĂ©nÄ› neĹľ 5 bajtĹŻ -> `PAYLOAD_TOO_SHORT`,
5. bajt 0 nenĂ­ `0x00` -> `INVALID_MAGIC_BYTE`,
6. bajty 1-4 se ÄŤtou jako big-endian `int` (schema ID),
7. ID nenĂ­ v mnoĹľinÄ› povolenĂ˝ch ID tohoto topicu -> `SCHEMA_NOT_ALLOWED`,
8. vĹˇechny recordy proĹˇly -> request se pĹ™epoĹˇle brokeru.

NavĂ­c: `MALFORMED_PRODUCE_REQUEST` (partition bez recordĹŻ, neÄŤitelnĂˇ nebo poĹˇkozenĂˇ dĂˇvka, control batch, request
bez topicĹŻ) a `INTERNAL_VALIDATION_ERROR` (jakĂˇkoli vĂ˝jimka pĹ™i validaci). Request se zamĂ­tĂˇ pĹ™i **prvnĂ­m** poruĹˇenĂ­.

### PodporovanĂ˝ Confluent wire format

```
byte 0      magic byte, vĹľdy 0
byte 1-4    schema ID, big-endian int32 (platnĂˇ ID jsou >= 0; hodnota se zĂˇpornĂ˝m znamĂ©nkem nikdy nenĂ­ povolenĂˇ)
byte 5+     serializovanĂ˝ payload: guardian ho neÄŤte
```

Rozhoduje **vĂ˝hradnÄ› schema ID**. Subject, verze a compatibility mode se neberou v Ăşvahu: je-li pro topic povoleno
jen ID 150 (`customer-value` v3), zprĂˇvy s ID 100 (v1) a 120 (v2) se odmĂ­tnou, i kdyĹľ majĂ­ stejnĂ˝ subject.

### AtomickĂˇ fail-closed politika

Jeden `ProduceRequest` mĹŻĹľe obsahovat vĂ­c topicĹŻ, partitions a `RecordBatch`. SelĹľe-li **jedinĂ˝** record, odmĂ­tne se
**celĂ˝ poĹľadavek** a brokeru se neposĂ­lĂˇ nic. Producer proto u zamĂ­tnutĂ© dĂˇvky nikdy nevidĂ­ ÄŤĂˇsteÄŤnĂ˝ ĂşspÄ›ch.

---

## 4. ChovĂˇnĂ­ vĹŻÄŤi producerĹŻm

PĹ™i odmĂ­tnutĂ­ dostane producer pro kaĹľdou partition `INVALID_RECORD` (kĂłd 87) s textem
`kafka-schema-guardian rejected the request: SCHEMA_NOT_ALLOWED topic=... partition=... recordIndex=... schemaId=...`
(nikdy obsah recordu).

`INVALID_RECORD` je **nereplikovatelnĂˇ** chyba: Java klient z nÄ›j vyrobĂ­ `InvalidRecordException`, kterĂˇ nenĂ­
`RetriableException`, a dĂˇvka selĹľe okamĹľitÄ›. Chyby, kterĂ© klient automaticky opakuje (`NETWORK_EXCEPTION`,
`REQUEST_TIMED_OUT`, `NOT_LEADER_OR_FOLLOWER`), se pro odmĂ­tnutĂ­ **nepouĹľĂ­vajĂ­**. Je to stejnĂˇ chyba, jakou vracĂ­
broker pĹ™i vlastnĂ­ validaci zĂˇznamu. JednotkovĂ˝ test `kafkaClientsTreatTheChosenErrorAsNonRetriable` ovÄ›Ĺ™uje tĹ™Ă­du
vĂ˝jimky proti `kafka-clients`.

| Situace | ChovĂˇnĂ­ |
|---|---|
| retries | zamĂ­tnutĂˇ dĂˇvka se neopakuje; poÄŤĂ­tĂˇ se jednou (viz test `recordWithDisallowedSchemaIdGetsNonRetriableErrorAndIsNotStored`) |
| idempotentnĂ­ producer | po selhĂˇnĂ­ dĂˇvky pokraÄŤuje dalĹˇĂ­ dĂˇvkou (ovÄ›Ĺ™uje `producerKeepsWorkingAfterARejection` v integraÄŤnĂ­ch testech s brokerem) |
| transakÄŤnĂ­ producer | chyba pĹ™evede transakci do *abortable* stavu: `commitTransaction()` selĹľe, aplikace volĂˇ `abortTransaction()` a zaÄŤne novou transakci |
| `acks=1`, `acks=all` | producer dostane odpovÄ›ÄŹ s `INVALID_RECORD` |
| **`acks=0`** | protokol pro `acks=0` odpovÄ›ÄŹ nemĂˇ. Proxy request zahodĂ­ (`drop`) a producer o odmĂ­tnutĂ­ **nezjistĂ­**. PoÄŤĂ­tĂˇ se v `kafka_schema_guardian_acks0_rejected_requests_total` a loguje (`acksZero=true`). Pro topicy s whitelistem `acks=0` zakaĹľte |
| komprimovanĂ© dĂˇvky | Kroxylicious record API dĂˇvku dekomprimuje (gzip, snappy, lz4, zstd); bez toho se dovnitĹ™ nedĂˇ nahlĂ©dnout |
| Produce v3-v13 | filtr nepĹ™epĂ­nĂˇ na `shouldHandleProduceRequest`, zpracuje vĹľdy; v13 (topic ID) je pokryt i integraÄŤnĂ­m testem |

ScĂ©nĂˇĹ™e s reĂˇlnĂ˝m producerem (idempotentnĂ­, transakÄŤnĂ­, komprese, `acks`) jsou v
[`SchemaGuardianProducerIT`](src/test/java/cz/company/guardian/it/SchemaGuardianProducerIT.java) a bÄ›ĹľĂ­ proti
skuteÄŤnĂ©mu brokeru (viz [kapitola 8](#8-build-a-testy)).

---

## 5. Konfigurace a whitelist

Filtr se v konfiguraci proxy odkazuje na soubor s pravidly:

```yaml
filterDefinitions:
  - name: schema-guardian
    type: SchemaGuardian
    config:
      rulesFile: /etc/kafka-schema-guardian/rules/rules.yaml
defaultFilters:
  - schema-guardian
```

Soubor s pravidly (v Kubernetes ConfigMap `kafka-schema-guardian-rules`, klĂ­ÄŤ `rules.yaml`):

```yaml
defaultPolicy: DENY

topics:
  TEST:
    allowedSchemaIds: [1, 2, 3]
  TEST2:
    allowedSchemaIds: [4, 5, 6]

metrics:
  includeSchemaIdLabel: false   # volitelnĂ©, vĂ˝chozĂ­ false
logging:
  maxRejectLogsPerSecond: 5     # volitelnĂ©, vĂ˝chozĂ­ 5
```

Konfigurace se ÄŤte a **pĹ™Ă­snÄ› validuje jednou pĹ™i startu**; chyba = proxy nenastartuje (pod zĹŻstane neready,
nic se neproxuje). Start selĹľe, pokud:

* `defaultPolicy` chybĂ­ nebo nenĂ­ pĹ™esnÄ› `DENY`,
* nenĂ­ definovĂˇn ĹľĂˇdnĂ˝ topic, nĂˇzev topicu je prĂˇzdnĂ˝ nebo nenĂ­ legĂˇlnĂ­ nĂˇzev Kafka topicu,
* topic nemĂˇ ĹľĂˇdnĂ© `allowedSchemaIds`, ID je `null`, zĂˇpornĂ©, desetinnĂ©, Ĺ™etÄ›zec nebo mimo rozsah `int`,
* soubor nenĂ­ validnĂ­ YAML/JSON, obsahuje neznĂˇmĂ˝ klĂ­ÄŤ, vĂ­ce YAML dokumentĹŻ, nebo je vÄ›tĹˇĂ­ neĹľ 1 MiB,
* je topic (nebo jakĂ˝koli klĂ­ÄŤ) definovĂˇn dvakrĂˇt (detekce duplicit pĹ™i parsovĂˇnĂ­),
* soubor neexistuje.

Whitelist se naÄŤte do nemÄ›nnĂ© struktury ([`Whitelist`](src/main/java/cz/company/guardian/validation/Whitelist.java):
`Map.copyOf` topic -> [`SchemaIdSet`](src/main/java/cz/company/guardian/validation/SchemaIdSet.java), nemÄ›nnĂˇ
open-addressing mnoĹľina primitivnĂ­ch `int`, vyhledĂˇnĂ­ O(1) bez boxovĂˇnĂ­). **Hot reload nenĂ­ implementovĂˇn**
(Kroxylicious 0.25.1 nemĂˇ sledovĂˇnĂ­ souborĹŻ a atomickĂ˝ reload pluginovĂ© konfigurace by byl vlastnĂ­ nespolehlivĂˇ
mechanika). ZmÄ›na whitelistu = rolling update, viz [kapitola 14](#14-zmÄ›na-whitelistu-rollback). Z toho plyne, Ĺľe
nejsou ani metriky reloadĹŻ.

Na hot path nenĂ­ ĹľĂˇdnĂ˝ vzdĂˇlenĂ˝ lookup, Schema Registry, disk, Kubernetes API, DNS, HTTP ani databĂˇze. JedinĂˇ
sĂ­ĹĄovĂˇ operace je pĹ™eklad topic ID na jmĂ©no (Produce v13+): proxy ho Ĺ™eĹˇĂ­ `Metadata` poĹľadavkem na broker a cachuje
per virtual cluster, ve filtru se volĂˇ jen pro ID, kterĂˇ v requestu nemajĂ­ jmĂ©no.

---

## 6. VĂ˝kon

* ÄŚte se pouze `buffer.get(position)` a `buffer.getInt(position + 1)`; `position`, `limit` ani `mark` se nemÄ›nĂ­
  (test `doesNotChangePositionLimitOrMark`). PoĹ™adĂ­ bajtĹŻ jinĂ© neĹľ big-endian se oĹˇetĹ™Ă­ bajt po bajtu.
* Hodnota se nikdy nekopĂ­ruje do `byte[]`. FungujĂ­ heap, **direct**, **read-only** i **sliced** buffery (testy).
* `Record.value()` v API Kroxylicious vracĂ­ `duplicate()` (malĂ˝ objekt, ĹľĂˇdnĂˇ kopie dat). U komprimovanĂ˝ch dĂˇvek musĂ­
  Kroxylicious dĂˇvku rozbalit, to je cena za moĹľnost se dĂ­vat dovnitĹ™.
* Na poĹľadavek vznikne jeden malĂ˝ vĂ˝slednĂ˝ objekt; na record ĹľĂˇdnĂˇ alokace v kĂłdu filtru. Lookup ve whitelistu
  neboxuje (`SchemaIdSet.contains(int)`).
* Metriky: ÄŤĂ­taÄŤe pro topicy z whitelistu se vytvoĹ™Ă­ pĹ™i startu; na hot pathu jen atomickĂˇ inkrementace. PoÄŤet
  ÄŤasovĂ˝ch Ĺ™ad je omezen (`__unconfigured__` pro neznĂˇmĂ© topicy, strop 1000 hodnot `schema_id`).
* Filtr je bezstavovĂ˝ a nemÄ›nnĂ˝, sdĂ­lĂ­ se jen `GuardianRuntime` (nemÄ›nnĂ˝) a atomickĂ© ÄŤĂ­taÄŤe.

---

## 7. LogovĂˇnĂ­ a metriky

### LogovĂˇnĂ­

StrukturovanĂ© zĂˇznamy pĹ™es SLF4J (`addKeyValue`), pĹ™i odmĂ­tnutĂ­ jen:
`event`, `timestamp`, `topic`, `partition`, `schemaId` (je-li ÄŤitelnĂ©), `reason`, `correlationId`, `sessionId`
(neprĹŻhlednĂ© ID spojenĂ­ vygenerovanĂ© proxy, bez adresy a certifikĂˇtu), `virtualCluster`.

Nikdy se neloguje payload, klĂ­ÄŤ, hlaviÄŤky, klientskĂ˝ certifikĂˇt ani hesla. JmĂ©no topicu pochĂˇzĂ­ od klienta, proto se
pĹ™ed logovĂˇnĂ­m sanitizuje (jen legĂˇlnĂ­ znaky, jinak `<invalid>`). U `INTERNAL_VALIDATION_ERROR` se loguje jen typ vĂ˝jimky
a mĂ­sto vyhozenĂ­, nikoli jejĂ­ zprĂˇva.

ZaplavenĂ­ logĹŻ brĂˇnĂ­ [`LogRateLimiter`](src/main/java/cz/company/guardian/logging/LogRateLimiter.java): nejvĂ˝Ĺˇe
`maxRejectLogsPerSecond` Ĺ™ĂˇdkĹŻ za sekundu na virtual cluster (vĂ˝chozĂ­ 5); nĂˇsledujĂ­cĂ­ Ĺ™Ăˇdek hlĂˇsĂ­
`suppressedSincePrevious`. HlavnĂ­m nĂˇstrojem pozorovatelnosti jsou metriky. S `KROXYLICIOUS_LOG_FORMAT=json` jdou
dvojice klĂ­ÄŤ-hodnota do JSON vĂ˝stupu proxy (u vĂ˝chozĂ­ Logstash Ĺˇablony typicky do `mdc`; strukturu si ovÄ›Ĺ™te ve svĂ©m
logovacĂ­m Ĺ™etÄ›zci).

### Prometheus metriky

RegistrujĂ­ se do globĂˇlnĂ­ho Micrometer registry proxy a vystavujĂ­ se na `:9190/metrics`.

| Metrika | Labely | VĂ˝znam |
|---|---|---|
| `kafka_schema_guardian_records_total` | `result=allowed\|rejected` | `allowed`: recordy v propuĹˇtÄ›nĂ˝ch requestech; `rejected`: prvnĂ­ poruĹˇujĂ­cĂ­ record (validace konÄŤĂ­ prvnĂ­ chybou) |
| `kafka_schema_guardian_rejections_total` | `topic`, `reason` (+ volitelnÄ› `schema_id`) | odmĂ­tnutĂ© requesty. NeznĂˇmĂ© topicy sdĂ­lĂ­ `topic="__unconfigured__"` |
| `kafka_schema_guardian_produce_requests_total` | `result=allowed\|rejected` | zpracovanĂ© `ProduceRequest`y |
| `kafka_schema_guardian_internal_errors_total` | | vĂ˝jimky pĹ™i validaci (request byl odmĂ­tnut) |
| `kafka_schema_guardian_acks0_rejected_requests_total` | | odmĂ­tnutĂ© requesty s `acks=0`, o nichĹľ producer nevĂ­ |
| `kafka_schema_guardian_validation_duration_seconds` | histogram | doba validace jednoho requestu (bez pĹ™ekladu topic ID) |
| `kafka_schema_guardian_configured_topics` | | poÄŤet topicĹŻ ve whitelistu |
| `kafka_schema_guardian_rules_info` | `checksum` | vĹľdy 1; `checksum` je krĂˇtkĂ˝ SHA-256 pravidel, rozdĂ­lnĂ© repliky poznĂˇte podle vĂ­ce hodnot |

Label `schema_id` je ve vĂ˝chozĂ­m stavu **vypnutĂ˝** (vysokĂˇ kardinalita). ZapĂ­nĂˇ se `metrics.includeSchemaIdLabel: true`;
pak se vytvĂˇĹ™Ă­ jen pro `SCHEMA_NOT_ALLOWED` a nejvĂ˝Ĺˇe 1000 hodnot, dalĹˇĂ­ se slouÄŤĂ­ do `schema_id="other"`. Sady labelĹŻ
tĂ©Ĺľe metriky musĂ­ bĂ˝t stejnĂ© pro vĹˇechny virtual clustery (Micrometer v proxy jinak registraci odmĂ­tne a proxy
nenastartuje).

UĹľiteÄŤnĂ© dotazy:

```promql
# odmĂ­tnutĂ© requesty podle topicu a dĹŻvodu
sum by (topic, reason) (rate(kafka_schema_guardian_rejections_total[5m]))
# rozdĂ­lnĂˇ pravidla mezi replikami (bÄ›hem rolloutu krĂˇtce normĂˇlnĂ­)
count(count by (checksum) (kafka_schema_guardian_rules_info)) > 1
# jakĂˇkoli internĂ­ chyba je incident
increase(kafka_schema_guardian_internal_errors_total[5m]) > 0
# p99 doby validace
histogram_quantile(0.99, sum by (le) (rate(kafka_schema_guardian_validation_duration_seconds_bucket[5m])))
```

---

## 8. Build a testy

PoĹľadavky: JDK 21, `make` (volitelnÄ›). Maven wrapper je v repozitĂˇĹ™i (`./mvnw`, Maven 3.9.9 s kontrolnĂ­m souÄŤtem).

```bash
make clean
make test        # unit testy, bez Dockeru a brokeru
make verify      # + integraÄŤnĂ­ testy + Enforcer + Spotless
make package     # target/kafka-schema-guardian-1.0.0.jar
make format      # Spotless apply
```

Build je reprodukovatelnĂ˝: fixnĂ­ `project.build.outputTimestamp`, pinnutĂ© verze vĹˇech pluginĹŻ, BOM
`kroxylicious-bom`/`jackson-bom`/`netty-bom`/`junit-bom`, Maven Enforcer (Java >= 21, Maven >= 3.9.6, `dependencyConvergence`,
zĂˇkaz SNAPSHOT a `log4j:log4j`), Surefire (`*Test`), Failsafe (`*IT`), Spotless (kontrola ve fĂˇzi `verify`).

### Testy

| Vrstva | TĹ™Ă­da | PokrytĂ­ |
|---|---|---|
| unit | `RecordCheckerTest` | povolenĂ©/nepovolenĂ© ID, null, < 5 bajtĹŻ, magic byte, ID 0, `Integer.MAX_VALUE`, zĂˇpornĂ© ID, **direct / read-only / sliced** buffer, nezmÄ›nÄ›nĂ˝ `position`/`limit`/`mark` |
| unit | `RequestValidatorTest` | vĂ­ce ID/topicĹŻ/partition, validnĂ­ mezi nevalidnĂ­mi a naopak, vĂ­ce batchĹŻ, komprese (gzip, lz4, snappy, zstd), idempotentnĂ­ a transakÄŤnĂ­ dĂˇvky, malformed, control batch, topic ID |
| unit | `SchemaGuardianFilterTest` | forward vs. `INVALID_RECORD`, atomickĂ© odmĂ­tnutĂ­, `acks`, **vĂ˝jimka pĹ™i validaci** -> odmĂ­tnutĂ­, selhĂˇnĂ­ i pĹ™i sestavenĂ­ odpovÄ›di -> stage selĹľe a nic se nepĹ™epoĹˇle, Produce v13 |
| unit | `RulesLoaderTest`, `ProxyConfigurationTemplatesTest` | vĹˇechny podmĂ­nky neĂşspÄ›ĹˇnĂ©ho startu, dodanĂ© konfigurace proxy projdou skuteÄŤnĂ˝m `ConfigParser` Kroxylicious |
| unit | `GuardianMetricsTest`, `PrometheusExpositionTest`, `RejectLoggerTest`, `LogRateLimiterTest` | nĂˇzvy metrik, kardinalita, rate limit, ĹľĂˇdnĂ˝ payload v logu |
| integraÄŤnĂ­ | `SchemaGuardianProxyIT` | **skuteÄŤnĂ˝ runtime Kroxylicious** pĹ™ed mock brokerem: odmĂ­tnutĂ˝ request broker neobdrĹľĂ­, atomicita, komprese, `acks=0`, Produce v13 s topic ID, restart, vĂ­ce replik, start s neplatnĂ˝mi pravidly |
| integraÄŤnĂ­ | `SchemaGuardianProducerIT`, `SchemaGuardianWireIT`, `SchemaGuardianResilienceIT` | **skuteÄŤnĂ˝ broker** (in-VM Kafka z `kroxylicious-testing`): `KafkaProducer` (idempotentnĂ­, transakÄŤnĂ­, komprese, `acks`, retries), syrovĂ© `ProduceRequest` pro verze 3, 7, 9, 12, 13 a vĂ­c topicĹŻ najednou, restart, 3 repliky |

IntegraÄŤnĂ­ testy s reĂˇlnĂ˝m brokerem jsou na Windows vypnutĂ© (`@DisabledOnOs`): in-VM Kafka tam padĂˇ na pĹ™ejmenovĂˇnĂ­
adresĂˇĹ™ĹŻ logu (KAFKA-1194). SpouĹˇtÄ›jĂ­ se v CI na Linuxu (`make verify`) nebo v kontejneru s
`TEST_CLUSTER_EXECUTION_MODE=CONTAINER`. `SchemaGuardianProxyIT` bÄ›ĹľĂ­ vĹˇude.

```bash
./mvnw verify -Dit.test=SchemaGuardianProxyIT   # jen integraÄŤnĂ­ testy bez brokeru
./mvnw verify -DskipITs                         # jen unit testy
```

---

## 9. Docker image

[Dockerfile](Dockerfile) je dvoustupĹovĂ˝:

1. `maven:3.9.9-eclipse-temurin-21` (pinnuto digestem): `mvn verify -DskipITs` (unit testy, Enforcer, Spotless) a JAR,
2. oficiĂˇlnĂ­ `quay.io/kroxylicious/proxy:0.25.1` (pinnuto digestem): pĹ™idĂˇ se JAR do `/opt/kroxylicious/libs/`, vĂ˝chozĂ­
   bezpeÄŤnĂˇ konfigurace (mTLS, pravidla `DENY` pro neexistujĂ­cĂ­ topic) a `USER 185` (non-root). Entrypoint
   (`tini` + `kroxylicious-start.sh`) zĹŻstĂˇvĂˇ z oficiĂˇlnĂ­ho image. Do image se nevklĂˇdajĂ­ ĹľĂˇdnĂ© certifikĂˇty ani klĂ­ÄŤe.

```bash
docker build -t kafka-schema-guardian:local .

# varianta jen s tagem (bez digestu)
docker build -t kafka-schema-guardian:local \
  --build-arg PROXY_IMAGE=quay.io/kroxylicious/proxy:0.25.1 \
  --build-arg BUILDER_IMAGE=maven:3.9.9-eclipse-temurin-21 .
```

Makefile (promÄ›nnĂ© `IMAGE_REGISTRY`, `IMAGE_REPOSITORY`, `IMAGE_TAG`, vĂ˝sledek `$(IMAGE_REGISTRY)/$(IMAGE_REPOSITORY):$(IMAGE_TAG)`):

```bash
make image IMAGE_TAG=1.0.0                         # lokĂˇlnĂ­ platforma
make image CONTAINER_ENGINE=podman IMAGE_TAG=1.0.0 # nebo buildah
make image-multiarch                               # linux/amd64 + linux/arm64

make image-push \
  IMAGE_REGISTRY=registry.example.com \
  IMAGE_REPOSITORY=platform/kafka-schema-guardian \
  IMAGE_TAG=1.0.0
```

Multi-arch image (`linux/amd64`, `linux/arm64`) stavĂ­ `docker buildx` (`--push`), u Podmanu/Buildahu manifest list
(`--manifest`, pak `manifest push --all`). Stage s Mavenem bÄ›ĹľĂ­ na platformÄ› build hostu, protoĹľe JAR je nezĂˇvislĂ˝ na
architektuĹ™e.

---

## 10. LokĂˇlnĂ­ testovĂˇnĂ­

```bash
make image IMAGE_REGISTRY=local IMAGE_REPOSITORY=kafka-schema-guardian IMAGE_TAG=local
docker compose -f examples/local/docker-compose.yaml up
```

JednouzlovĂ˝ Kafka (KRaft) dostupnĂ˝ **jen** pĹ™es guardian (`localhost:9192`; port brokeru se nepublikuje). Whitelist je
v [examples/local/rules.yaml](examples/local/rules.yaml): topic `orders`, ID 1 a 2.

```bash
# povoleno (magic byte 0, schema ID 1)
printf '\x00\x00\x00\x00\x01hello' | kafka-console-producer.sh --bootstrap-server localhost:9192 --topic orders
# zamĂ­tnuto: ID 9 nenĂ­ ve whitelistu, producer dostane INVALID_RECORD
printf '\x00\x00\x00\x00\x09hello' | kafka-console-producer.sh --bootstrap-server localhost:9192 --topic orders
curl -s localhost:9190/metrics | grep kafka_schema_guardian
```

---

## 11. Kubernetes: Kustomize a Helm

Namespace `kafka-proxy` (guardian), Strimzi v namespace `kafka` (cluster `my-cluster`). Hodnoty upravte v
[`deploy/proxy-config.yaml`](deploy/proxy-config.yaml) nebo v `values.yaml`.

| Soubor | Obsah |
|---|---|
| [`deploy/namespace.yaml`](deploy/namespace.yaml) | Namespace (Pod Security `restricted`) |
| [`deploy/rules.yaml`](deploy/rules.yaml) | whitelist; Kustomize z nÄ›j generuje ConfigMap `kafka-schema-guardian-rules` |
| [`deploy/proxy-config.yaml`](deploy/proxy-config.yaml) | konfigurace proxy -> ConfigMap `kafka-schema-guardian-config` (vĂ˝chozĂ­ mTLS) |
| [`deploy/deployment.yaml`](deploy/deployment.yaml) | 3 repliky, RollingUpdate, probes, resources, securityContext, topologySpread, anti-affinity |
| [`deploy/service.yaml`](deploy/service.yaml) | `kafka-schema-guardian-bootstrap` (Kafka porty) a `kafka-schema-guardian-management` (health, metriky) |
| [`deploy/poddisruptionbudget.yaml`](deploy/poddisruptionbudget.yaml), [`deploy/networkpolicy.yaml`](deploy/networkpolicy.yaml) | PDB (`maxUnavailable: 1`), NetworkPolicy |
| [`deploy/secret-example.yaml`](deploy/secret-example.yaml) | **jen placeholdery** ÄŤtyĹ™ SecretĹŻ, do kustomization nepatĹ™Ă­ |
| [`deploy/optional/`](deploy/optional) | `servicemonitor.yaml` (Prometheus Operator), `hpa.yaml` |
| [`deploy/overlays/tls`](deploy/overlays/tls), [`deploy/overlays/plaintext`](deploy/overlays/plaintext) | varianty konfigurace proxy |
| [`deploy/helm/kafka-schema-guardian-chart`](deploy/helm/kafka-schema-guardian-chart) | Helm chart se stejnĂ˝m obsahem; publikuje se na Docker Hub jako OCI artefakt |

### Kustomize

```bash
kubectl apply -f deploy/namespace.yaml
# vytvoĹ™te Secrety (viz deploy/secret-example.yaml a kapitolu 13), pak:
make deploy IMAGE_REGISTRY=registry.example.com IMAGE_REPOSITORY=platform/kafka-schema-guardian IMAGE_TAG=1.0.0
make deploy KUSTOMIZE_DIR=deploy/overlays/tls        # varianta TLS
make deploy-optional                                 # ServiceMonitor + HPA
make undeploy
```

Kustomize generuje oba ConfigMapy s hashem v nĂˇzvu, takĹľe **Ăşprava `rules.yaml` nebo `proxy-config.yaml` zmÄ›nĂ­ pod
template a spustĂ­ RollingUpdate** (obdoba checksum anotace). Pody neprojdou readiness, dokud proxy neotevĹ™e Kafka porty,
a to probÄ›hne aĹľ po ĂşspÄ›ĹˇnĂ©m naÄŤtenĂ­ a validaci whitelistu.

### Helm

```bash
# z Docker Hubu (publikuje pipeline, viz kapitola 16); <ns> = vĂˇĹˇ Docker Hub namespace
helm upgrade --install kafka-schema-guardian oci://registry-1.docker.io/<ns>/kafka-schema-guardian-chart \
  --version 1.0.0 \
  --namespace kafka-proxy --create-namespace \
  --set mode=mtls \
  --set kafka.clusterName=my-cluster --set kafka.namespace=kafka \
  -f my-values.yaml          # rules.topics, nodeIdRanges, ...

# nebo z pracovnĂ­ kopie
helm upgrade --install kafka-schema-guardian deploy/helm/kafka-schema-guardian-chart \
  --namespace kafka-proxy --create-namespace \
  --set image.repository=docker.io/<ns>/kafka-schema-guardian \
  --set image.tag=1.0.0 \
  -f my-values.yaml
```

BalĂ­ÄŤek z Docker Hubu mĂˇ `image.repository` uĹľ nastavenĂ˝ na image publikovanĂ˝ stejnĂ˝m bÄ›hem pipeline a `image.tag`
odpovĂ­dĂˇ verzi chartu (`appVersion`). NĂˇzev chartu je `kafka-schema-guardian-chart`, aby nekolidoval s repozitĂˇĹ™em image;
nĂˇzvy a labely Kubernetes zdrojĹŻ zĹŻstĂˇvajĂ­ `kafka-schema-guardian` (`nameOverride`).

Chart vyĹľaduje v `values.yaml` aspoĹ `rules.topics` (jinak `helm template` skonÄŤĂ­ chybou), poÄŤĂ­tĂˇ porty brokerĹŻ z
`proxy.nodeIdRanges` a do pod template dĂˇvĂˇ anotace `checksum/proxy-config` a `checksum/rules`. OvÄ›Ĺ™eno
`helm lint` a `helm template` (Helm 4.3.1); `kubectl kustomize` ani nasazenĂ­ na cluster v tomto repozitĂˇĹ™i ovÄ›Ĺ™eny nebyly.

### VĂ˝chozĂ­ zdroje

```yaml
requests: {cpu: 500m, memory: 512Mi}
limits:   {cpu: "2",  memory: 2Gi}
```

Jsou to vĂ˝chozĂ­ hodnoty bez zmÄ›Ĺ™enĂ­ na vaĹˇem provozu. CPU spotĹ™ebovĂˇvajĂ­ TLS, dekomprese dĂˇvek a Netty; pamÄ›ĹĄ
heap (`JAVA_MAX_MEM_RATIO=60`) plus direct buffery Netty. Postup: spustit zĂˇtÄ›Ĺľ, sledovat `container_cpu_usage_seconds_total`
a `container_memory_working_set_bytes`, `requests` nastavit na typickou zĂˇtÄ›Ĺľ, `limits.memory` s rezervou nad ĹˇpiÄŤku,
a ĹˇkĂˇlovat poÄŤtem replik (HPA na CPU 70 %). PĹ™i `OOMKilled` zvyĹˇte `limits.memory` nebo sniĹľte `JAVA_MAX_MEM_RATIO`.

---

## 12. SĂ­tÄ›, Kafka metadata a Strimzi

Kafka klient po bootstrapu dostane v `Metadata` adresy brokerĹŻ a pĹ™ipojuje se na nÄ› pĹ™Ă­mo. Proto nestaÄŤĂ­ proxovat
jen bootstrap. Kroxylicious proto **pĹ™episuje adresy brokerĹŻ** (virtual cluster, gateway `portIdentifiesNode`):

```yaml
virtualClusters:
  - name: guardian
    target: {cluster: strimzi}
    gateways:
      - name: mtls
        portIdentifiesNode:
          bootstrapAddress: kafka-schema-guardian-bootstrap.kafka-proxy.svc.cluster.local:9092
          advertisedBrokerAddressPattern: kafka-schema-guardian-bootstrap.kafka-proxy.svc.cluster.local
          nodeStartPort: 9093
          nodeIdRanges:
            - {name: brokers, start: 0, end: 2}      # node ID Strimzi brokerĹŻ, rozsah je vÄŤetnÄ›
```

| Co | Hodnota |
|---|---|
| Bootstrap adresa pro producery | `kafka-schema-guardian-bootstrap.kafka-proxy.svc.cluster.local:9092` |
| Adresy brokerĹŻ, kterĂ© klient dostane | stejnĂ˝ DNS nĂˇzev, port `9093` (node 0), `9094` (node 1), `9095` (node 2) |
| Upstream (Strimzi bootstrap) | `my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9093` |
| Endpointy pro jednotlivĂ© brokery | ĹľĂˇdnĂ© samostatnĂ© Service: `kafka-schema-guardian-bootstrap` vystavuje vĹˇechny porty a **kaĹľdĂˇ replika obslouĹľĂ­ kaĹľdĂ˝ port** (broker se poznĂˇ podle portu) |
| VnitĹ™nĂ­ pĹ™Ă­stup | DNS Service uvnitĹ™ clusteru; vnÄ›jĹˇĂ­ pĹ™Ă­stup by vyĹľadoval LoadBalancer/Ingress s `sniHostIdentifiesNode` (vlastnĂ­ hostname, wildcard certifikĂˇt) |

Node ID musĂ­ odpovĂ­dat skuteÄŤnosti (`kubectl -n kafka get pods -l strimzi.io/kind=Kafka`, pĹ™Ă­padnÄ› `KafkaNodePool`);
poÄŤet portĹŻ v Service, Deploymentu a NetworkPolicy se musĂ­ shodovat s `nodeIdRanges` (test
`servicePortsMatchTheGatewayPorts`; v Helm chartu se poÄŤĂ­tajĂ­ automaticky). PĹ™ibude-li broker, pĹ™idejte jeho node ID
a port.

### ZabrĂˇnÄ›nĂ­ obejitĂ­ proxy

* Klient dostane z `Metadata` jen adresy proxy; broker svou vnitĹ™nĂ­ adresu klientovi neĹ™ekne.
* Brokery nesmĂ­ bĂ˝t dosaĹľitelnĂ© pro producery. NejlĂ©pe pĹ™Ă­mo ve Strimzi (`networkPolicyPeers` listeneru; Strimzi
  vygeneruje NetworkPolicy):

```yaml
# Kafka CR, namespace kafka
spec:
  kafka:
    listeners:
      - name: tls
        port: 9093
        type: internal
        tls: true
        authentication: {type: tls}
        networkPolicyPeers:
          - namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: kafka-proxy}}
            podSelector: {matchLabels: {app.kubernetes.io/name: kafka-schema-guardian}}
```

* Proxy mĂˇ NetworkPolicy: ingress jen z namespace s labelem `kafka-schema-guardian/access=allowed`, egress jen na DNS a
  na Strimzi brokery.

### Strimzi: pĹ™ipojenĂ­ proxy

1. `KafkaUser` typu `tls` pro proxy (ACL `Write`, `Describe`, `Create` na chrĂˇnÄ›nĂ˝ch topicĂ­ch; viz limitace identity nĂ­Ĺľe).
2. ZkopĂ­rujte do namespace `kafka-proxy`: `my-cluster-cluster-ca-cert` (klĂ­ÄŤ `ca.crt`) jako Secret
   `kafka-schema-guardian-upstream-ca` a Secret `KafkaUser`a (`user.crt`, `user.key`) jako
   `kafka-schema-guardian-upstream-client` (klĂ­ÄŤe `tls.crt`, `tls.key`).
3. `bootstrapServers`, namespace a jmĂ©no clusteru jsou v `clusterDefinitions` (Helm: `kafka.clusterName`, `kafka.namespace`,
   `kafka.bootstrapHost`, `kafka.bootstrapPort`, `kafka.tls.*`).

Guardian nemÄ›nĂ­ Strimzi image, nebÄ›ĹľĂ­ v brokerech a nevytvĂˇĹ™Ă­ vlastnĂ­ image brokeru.

---

## 13. PLAINTEXT, TLS a mTLS

PouĹľĂ­vĂˇ se vĂ˝hradnÄ› TLS vrstva Kroxylicious; certifikĂˇty se naÄŤĂ­tajĂ­ z Kubernetes SecretĹŻ (volume mounty,
`readOnly`), **nikdy ne z image**.

| Varianta | Jak |
|---|---|
| mTLS (vĂ˝chozĂ­) | `deploy/` nebo Helm `mode=mtls` |
| TLS | `deploy/overlays/tls` nebo `mode=tls` |
| PLAINTEXT | `deploy/overlays/plaintext` nebo `mode=plaintext` (jen dĹŻvÄ›ryhodnĂˇ sĂ­ĹĄ) |
| Proxy -> Strimzi | TLS/mTLS vĹľdy (`clusterDefinitions[].tls`) |

| Secret | KlĂ­ÄŤe | PouĹľitĂ­ |
|---|---|---|
| `kafka-schema-guardian-server-tls` | `tls.crt`, `tls.key` | certifikĂˇt, kterĂ˝ proxy pĹ™edklĂˇdĂˇ producerĹŻm (TLS server). SAN musĂ­ obsahovat `kafka-schema-guardian-bootstrap.kafka-proxy.svc.cluster.local` |
| `kafka-schema-guardian-client-ca` | `ca.crt` | CA, jĂ­ ovÄ›Ĺ™uje klientskĂ© certifikĂˇty producerĹŻ (`trustOptions.clientAuth: REQUIRED`) |
| `kafka-schema-guardian-upstream-ca` | `ca.crt` | CA brokerĹŻ (proxy jako TLS klient) |
| `kafka-schema-guardian-upstream-client` | `tls.crt`, `tls.key` | certifikĂˇt proxy vĹŻÄŤi brokerĹŻm (mTLS) |

### Rotace SecretĹŻ

Kroxylicious 0.25.1 naÄŤĂ­tĂˇ soubory s certifikĂˇty a klĂ­ÄŤi **pĹ™i startu virtual clusteru** a neimplementuje jejich
automatickĂ© znovunaÄŤtenĂ­ (v runtime nenĂ­ ĹľĂˇdnĂ© sledovĂˇnĂ­ souborĹŻ). Kubernetes sice obsah mountovanĂ©ho Secretu
aktualizuje, ale proxy dĂˇl pouĹľĂ­vĂˇ pĹŻvodnĂ­. Po rotaci je proto nutnĂ˝ rolling restart:

```bash
kubectl -n kafka-proxy rollout restart deployment/kafka-schema-guardian
```

Rolling restart je nutnĂ˝ pĹ™i zmÄ›nÄ› SecretĹŻ s certifikĂˇty/klĂ­ÄŤi, zmÄ›nÄ› `proxy-config.yaml` (Kustomize/Helm to udÄ›lajĂ­
samy pĹ™es hash/checksum), zmÄ›nÄ› `rules.yaml` a pĹ™i zmÄ›nÄ› image. DoporuÄŤenĂ­: certifikĂˇty vydĂˇvejte pĹ™edem (cert-manager)
a obmÄ›Ĺujte je s dostateÄŤnĂ˝m pĹ™edstihem pĹ™ed expiracĂ­; rotaci SecretĹŻ mĹŻĹľete automatizovat nĂˇstrojem typu Reloader.

---

## 14. ZmÄ›na whitelistu, rollback

1. Upravte `deploy/rules.yaml` (nebo `rules.topics` v Helm values). Validace probÄ›hne pĹ™i startu kaĹľdĂ©ho podu.
2. Aplikujte (`make deploy` / `helm upgrade`). NovĂ˝ ConfigMap mĂˇ jinĂ˝ hash, Deployment se rozjede rolling updatem
   (`maxUnavailable: 0`, `maxSurge: 1`): novĂ˝ pod musĂ­ projĂ­t readiness (pravidla naÄŤtena a validnĂ­), teprve pak se
   ukonÄŤĂ­ starĂ˝. **NeplatnĂ© pravidla tedy rollout zastavĂ­ a bÄ›ĹľĂ­cĂ­ repliky zĹŻstanou na starĂ˝ch pravidlech.**
3. Kontrola: `kafka_schema_guardian_rules_info{checksum}` musĂ­ mĂ­t po rolloutu jedinou hodnotu.

Rollback: `kubectl -n kafka-proxy rollout undo deployment/kafka-schema-guardian` (vracĂ­ pĹ™edchozĂ­ pod template a tĂ­m
i pĹ™edchozĂ­ ConfigMap), nebo `helm rollback kafka-schema-guardian`, nebo nĂˇvrat zmÄ›ny v Gitu a novĂ© nasazenĂ­.
StarĹˇĂ­ ConfigMapy ponechte (Kustomize je pĹ™i `apply` nemaĹľe); rollback je potĹ™ebuje.

BÄ›hem rolloutu mohou krĂˇtce koexistovat repliky se starĂ˝m a novĂ˝m whitelistem; producer se pĹ™i reconnectu dostane na
libovolnou. PĹ™idĂˇvĂˇte-li schema ID, nasazujte whitelist **pĹ™ed** producery, kterĂ© ho zaÄŤnou pouĹľĂ­vat.

---

## 15. Dostupnost, ĹˇkĂˇlovĂˇnĂ­, nastavenĂ­ producerĹŻ

* `replicas: 3`, `RollingUpdate`, `topologySpreadConstraints` (zĂłna a node), preferovanĂˇ `podAntiAffinity`,
  `PodDisruptionBudget` (`maxUnavailable: 1`), `startupProbe`, `readinessProbe`, `livenessProbe`,
  `terminationGracePeriodSeconds: 45`.
* **Graceful shutdown**: `preStop` poÄŤkĂˇ 10 s (Service stihne pod odebrat z endpointĹŻ), pak SIGTERM; proxy pĹ™estane
  pĹ™ijĂ­mat spojenĂ­ a ÄŤekĂˇ aĹľ `drainTimeout` (10 s) na rozpracovanĂ© requesty, potom zavĹ™e event loopy
  (`shutdownQuietPeriod: 2s`, `shutdownTimeout: 15s`).
* Probes: proxy mĂˇ jedinĂ˝ health endpoint `/livez` (port 9190). Liveness a startup ho pouĹľĂ­vajĂ­. **Readiness je TCP
  na bootstrap port**, kterĂ˝ se otevĹ™e aĹľ po ĂşspÄ›ĹˇnĂ©m startu virtual clusteru, tj. po naÄŤtenĂ­ whitelistu.
* HPA (volitelnÄ›) na CPU, `optional/hpa.yaml` nebo `autoscaling.enabled=true`.

### Dlouho ĹľijĂ­cĂ­ spojenĂ­

NovĂ© repliky pĹ™i scale-outu **nepĹ™evezmou existujĂ­cĂ­ spojenĂ­**: Kafka spojenĂ­ je dlouhodobĂ© a TCP se mezi pody
nepĹ™ebalancuje. NovĂˇ replika dostane jen novĂˇ spojenĂ­ (reconnect, novĂ˝ producer). Proto je ve vĂ˝chozĂ­ konfiguraci filtr
`ConnectionExpiration` (`maxAge: 30m`, `jitter: 5m`): proxy po uplynutĂ­ stĂˇĹ™Ă­ spojenĂ­ zavĹ™e a producer se pĹ™es Service
pĹ™ipojĂ­ znovu, tedy rovnomÄ›rnÄ›ji. Je to Ĺ™Ă­zenĂ˝ reconnect; kratĹˇĂ­ `maxAge` = rovnomÄ›rnÄ›jĹˇĂ­ rozloĹľenĂ­ za cenu ÄŤastÄ›jĹˇĂ­ch
reconnectĹŻ. Vypnete ho odebrĂˇnĂ­m filtru z `filterDefinitions`/`defaultFilters` (Helm: `proxy.connectionExpiration.enabled=false`).

### Dopad rolling updatu

Pod s bÄ›ĹľĂ­cĂ­mi spojenĂ­mi se ukonÄŤĂ­, producer dostane odpojenĂ­ a znovu navĂˇĹľe spojenĂ­ pĹ™es bootstrap. S idempotentnĂ­m
producerem (vĂ˝chozĂ­ v klientech 3.x+) jsou opakovĂˇnĂ­ bezpeÄŤnĂˇ (bez duplicit); s `acks=0` se rozpracovanĂ© dĂˇvky ztratĂ­.

### DoporuÄŤenĂ© nastavenĂ­ producerĹŻ (bez zmÄ›ny aplikaÄŤnĂ­ho kĂłdu)

```properties
# klientskĂ© konfigurace, typicky pĹ™es externĂ­ soubor/ENV
bootstrap.servers=kafka-schema-guardian-bootstrap.kafka-proxy.svc.cluster.local:9092
enable.idempotence=true
acks=all
retries=2147483647
delivery.timeout.ms=120000        # musĂ­ pĹ™eklenout rolling update (~1 minuta)
request.timeout.ms=30000
retry.backoff.ms=200
retry.backoff.max.ms=2000
reconnect.backoff.ms=200
reconnect.backoff.max.ms=5000
metadata.max.age.ms=60000
```

`INVALID_RECORD` se neopakuje a musĂ­ ho aplikace zpracovat jako chybu odeslĂˇnĂ­ (callback / `Future`).

---

## 16. CI/CD

Pipeline [`.github/workflows/build.yaml`](.github/workflows/build.yaml) sestaví a otestuje projekt a publikuje na
**Docker Hub** image i Helm chart:

| Artefakt | Umístění |
|---|---|
| image (`linux/amd64`, `linux/arm64`) | `docker.io/<ns>/kafka-schema-guardian` |
| Helm chart (OCI) | `oci://registry-1.docker.io/<ns>/kafka-schema-guardian-chart` |

Kroky: `verify` (kompilace, unit a integrační testy, Enforcer, Spotless, CycloneDX SBOM, JAR) a `manifests`
(`helm lint`, `helm package`, `kubectl kustomize`, `kubeconform`) -> `image` (build, **Trivy sken** HIGH/CRITICAL
s neúspěchem, **SBOM image**, publikace) -> `chart` (balíček, push, kontrola `helm show chart`). Publikuje se jen
mimo pull requesty a jen po úspěchu všech předchozích kroků; PR nepotřebují žádná tajemství.

| Událost | Tagy image a verze chartu |
|---|---|
| push do `main` nebo `master` | `1.0.0-g<commit>` (neměnný; pro chart validní SemVer pre-release) |
| tag `v1.0.0` | totéž a navíc `1.0.0` (tag se musí rovnat verzi v `pom.xml`, jinak pipeline selže) |

Image se nikdy netaguje `latest`. Chart i image mají stejnou verzi, chart ukazuje na image se svou `appVersion`.

### Nastavení repozitáře (GitHub -> Settings -> Secrets and variables -> Actions)

1. Docker Hub -> Account settings -> Personal access tokens: token s právy *Read & Write*.
2. Secrets: `DOCKERHUB_USERNAME`, `DOCKERHUB_TOKEN`.
3. Variables (volitelné): `DOCKERHUB_NAMESPACE` (uživatel nebo organizace; výchozí je `DOCKERHUB_USERNAME`),
   `COSIGN_ENABLED=true` (keyless podpis image a chartu přes OIDC).
4. Vydání: změňte verzi v `pom.xml` a `Chart.yaml`, `git tag v1.0.0 && git push origin v1.0.0`.

Repozitáře `kafka-schema-guardian` a `kafka-schema-guardian-chart` Docker Hub při prvním pushi vytvoří jako veřejné;
privátní vytvořte předem.

Ruční publikace stejnými příkazy jako pipeline:

```bash
docker login
make image-push IMAGE_REGISTRY=docker.io IMAGE_REPOSITORY=<ns>/kafka-schema-guardian IMAGE_TAG=1.0.0
helm registry login registry-1.docker.io -u <user>
make helm-push IMAGE_REGISTRY=docker.io IMAGE_REPOSITORY=<ns>/kafka-schema-guardian IMAGE_TAG=1.0.0
```

Veškerá logika je v `make` cílech a obyčejných příkazech, takže převod do GitLab CI nebo Azure DevOps je přepsání kroků
jedna k jedné. Ověření podpisu: `cosign verify --certificate-identity-regexp '...' --certificate-oidc-issuer https://token.actions.githubusercontent.com docker.io/<ns>/kafka-schema-guardian:<tag>`.

Pipeline jsem nespustil (chybí GitHub ani Docker); Helm balíček jsem ověřil lokálně (`helm lint`, `helm package`).

---
## 17. Troubleshooting

| PĹ™Ă­znak | PĹ™Ă­ÄŤina / postup |
|---|---|
| Pod `CrashLoopBackOff`, v logu `Invalid rules ...` | whitelist je neplatnĂ˝ (viz seznam podmĂ­nek v kapitole 5); zprĂˇva vypĂ­Ĺˇe vĹˇechny nalezenĂ© chyby |
| Pod `CrashLoopBackOff`, `does not exist` | chybĂ­ soubor v mountu (Secret s certifikĂˇtem, `rules.yaml`); pro zvolenĂ˝ reĹľim chybĂ­ Secret |
| Pod `ContainerCreating` | chybĂ­ povinnĂ˝ Secret (`upstream-ca`, `upstream-client`) |
| Producer: `InvalidRecordException ... kafka-schema-guardian: ...` | zĂˇznam nesplĹuje whitelist; `reason` ve zprĂˇvÄ› Ĺ™Ă­kĂˇ proÄŤ (`SCHEMA_NOT_ALLOWED`, `INVALID_MAGIC_BYTE`, ...) |
| Producer visĂ­ na `Metadata` / `TimeoutException` | DNS nĂˇzev z `advertisedBrokerAddressPattern` nelze pĹ™eloĹľit, port brokeru nenĂ­ v Service/NetworkPolicy, nebo `nodeIdRanges` neodpovĂ­dĂˇ node ID Strimzi |
| TLS handshake selĹľe | SAN certifikĂˇtu neobsahuje advertised hostname; v mTLS chybĂ­/nevÄ›Ĺ™Ă­ se klientskĂ©mu certifikĂˇtu |
| `kafka_schema_guardian_internal_errors_total` roste | vĂ˝jimka ve validaci: request je odmĂ­tnut; najdÄ›te `validation_error` v logu (`exception`, `location`) |
| Rejecty bez odpovÄ›di u producera | `acks=0`: viz `kafka_schema_guardian_acks0_rejected_requests_total` |
| Po rolloutu jinĂ© chovĂˇnĂ­ mezi replikami | `count(count by (checksum) (kafka_schema_guardian_rules_info))`; poÄŤkejte na dokonÄŤenĂ­ rolloutu |
| Po rotaci certifikĂˇtu proxy stĂˇle pĹ™edklĂˇdĂˇ starĂ˝ | proxy certifikĂˇty znovu nenaÄŤĂ­tĂˇ; `kubectl rollout restart` |

ZapnutĂ­ podrobnÄ›jĹˇĂ­ho logu: `KROXYLICIOUS_APP_LOG_LEVEL=DEBUG` (pozor, DEBUG logy Kroxylicious mohou obsahovat
metadata spojenĂ­; payload se neloguje).

---

## 18. BezpeÄŤnostnĂ­ omezenĂ­ a znĂˇmĂ© limitace

* **Guardian kontroluje jen strukturu hlaviÄŤky.** NeovÄ›Ĺ™Ă­, Ĺľe ID existuje v registru, Ĺľe payload odpovĂ­dĂˇ schĂ©matu, ani
  Ĺľe producer schĂ©ma skuteÄŤnÄ› pouĹľil. Producer, kterĂ˝ poĹˇle `0x00` + povolenĂ© ID a libovolnĂ˝ payload, projde. Je to
  zĂˇmÄ›r (rychlost, jednoduchost); jako bezpeÄŤnostnĂ­ hranici proti zlovolnĂ©mu producerovi to samo nestaÄŤĂ­.
* Ochrana funguje jen pro provoz **pĹ™es proxy**: brokery musĂ­ bĂ˝t pro producery nedosaĹľitelnĂ© (kapitola 12).
* Broker vidĂ­ vĹˇechny producery jako **jednu identitu proxy** (`KafkaUser` proxy). Per-producer ACL na brokeru nejsou
  moĹľnĂ©; omezenĂ­ per-producer vyĹľaduje Kroxylicious autorizaÄŤnĂ­ filtr nebo jinĂ˝ mechanismus pĹ™ed guardianem.
* `acks=0`: odmĂ­tnutĂ­ producer neuvidĂ­ (kapitola 4).
* RozhodovĂˇnĂ­ se opĂ­rĂˇ o **prvnĂ­** poruĹˇenĂ­; metrika `records_total{result="rejected"}` proto poÄŤĂ­tĂˇ jen to jedno.
* KomprimovanĂ© dĂˇvky se musĂ­ dekomprimovat: CPU cena. Control batche od klienta se odmĂ­tajĂ­.
* Hot reload whitelistu a certifikĂˇtĹŻ nenĂ­. NeplatnĂˇ novĂˇ konfigurace tak nikdy neovlivnĂ­ bÄ›ĹľĂ­cĂ­ repliky.
* Plugin zĂˇvisĂ­ na nepodporovanĂ©m (`...record.internal`) API Kroxylicious: vĹľdy aktualizujte spoleÄŤnÄ› s proxy (kapitola 1).
* PĹ™eklad topic ID (Produce v13+) pouĹľĂ­vĂˇ `Metadata` poĹľadavek proxy na broker; pĹ™i nedostupnĂ©m brokeru se request
  zamĂ­tne (`UNKNOWN_TOPIC_ID`, klient ho zopakuje po obnovÄ› metadat), nikdy neprojde.
* Kroxylicious `shortCircuitResponse` u `acks=0` odpovÄ›ÄŹ zahazuje; guardian proto v tom pĹ™Ă­padÄ› volĂˇ `drop()`.
* OvÄ›Ĺ™enĂ­: integraÄŤnĂ­ testy s reĂˇlnĂ˝m brokerem se vĂ˝vojovÄ› nebÄ›Ĺľely na Windows (viz kapitola 8) a image se v tomto
  prostĹ™edĂ­ nesestavoval (chybĂ­ Docker); jednotkovĂ© testy a `SchemaGuardianProxyIT` proti skuteÄŤnĂ©mu runtime Kroxylicious bÄ›ĹľĂ­.
