# Bifrost в Labpics

Этот репозиторий — поддерживаемый форк [maximhq/bifrost](https://github.com/maximhq/bifrost).
Рабочая ветка — `labpics`. Исходная лицензия [Apache 2.0](LICENSE) сохранена.
База upstream и обязательные регрессии объявлены в [ci/verification.json](ci/verification.json).
Исходники core, framework, plugins и HTTP transport изменяются здесь обычными PR.

Первичный импорт воспроизвёл поведение сборки из
[Labpics Infra `21660ec01`](https://github.com/Labpics-Team/labpics-infra/tree/21660ec01806179dea062f54904c0e995678acc1/services/bifrost/build):
восемь действующих патчей стали исходниками вместе с их тестами. Дерево после
применения прежнего набора — `30ab5f078c34c03eab90c32dce56a727a1060161`.
При проверке импорта пересмотрены отличия, которые подменяли штатные настройки
upstream; действующие границы описаны ниже.

Для выпусков Infra использует исходники этого форка: закрепляет коммит в
[source.json](https://github.com/Labpics-Team/labpics-infra/blob/main/services/bifrost/build/source.json),
собирает проверенный образ и публикует его в GHCR. Внешний набор патчей Infra
удалён при [переходе на форк](https://github.com/Labpics-Team/labpics-infra/pull/2683).
Merge в `labpics` запускает проверки форка, но сам по себе не выпускает образ
и не обновляет работающий сервис.

## Почему изменения сохранены

### Диагностика запуска потока

Существующий span LLM-вызова или retry сохраняет только числовые атрибуты
`bifrost.stream.start.*`. Счётчики принадлежат одной попытке и прекращают рост
при решении штатной проверки выпустить поток; snapshot записывается один раз
перед завершением span. Отложенный finalizer предыдущей попытки его не меняет.
Диагностика не меняет таймауты, retries, fallback, кэш, SSE framing или post-hooks.

| Суффикс атрибута | Смысл |
|---|---|
| `http_status`, `headers_ms` | Статус первого успешного `DoStreamingRequest` и время самого вызова до заголовков; включает подключение и отправку запроса |
| `body_bytes`, `last_read_ms` | Байты, возвращённые существующим idle reader, и время последнего чтения с `n > 0` от начала попытки |
| `sse_comments`, `sse_data_lines` | Комментарии и строки `data:`, распознанные штатным SSE reader до commit; строки включают пустые данные и `[DONE]`, не доказывают успешный JSON decode |
| `preamble_chunks` | Результаты после post-hooks, которые действующий native classifier признал preamble, включая chunk, превысивший лимит буфера |
| `commit_ms`, `commit_reason` | Время решения startup checker от начала попытки; `1` — принят результат вне preamble (или classifier отсутствует), `2` — выпуск при переполнении preamble без TTFT deadline |

`commit` означает решение выпустить поток, а не запись клиенту и не обязательно
первый полезный токен. В частности, причина `2` не подтверждает полезный ответ.
Bytes измеряются после декомпрессии и могут включать данные, прочитанные вперёд
в один буфер до обработки commit. Это не TCP/TLS bytes.

Нулевые счётчики присутствуют, если соответствующий reader был установлен.
Отсутствие атрибута означает, что эта точка не была достигнута или не измеряется:
`http_status` относится к fasthttp streaming path, SSE-счётчики — к штатным
readers, а не к подменённому `SSEReaderFactory`. `last_read_ms` и `commit_ms`
отсутствуют, если соответствующее событие не произошло.

Для зависшей попытки с HTTP 200: `body_bytes=0` отличает тишину тела;
comments без data показывают keepalive; data вместе с preamble без commit
показывают стартовые события без выпуска потока. Содержимое событий, headers,
URL и новые метки Prometheus эта диагностика не сохраняет.

Основания: [WHATWG SSE](https://html.spec.whatwg.org/multipage/server-sent-events.html#parsing-an-event-stream)
отделяет комментарии от событий;
[OpenTelemetry Tracing API](https://opentelemetry.io/docs/specs/otel/trace/api/)
требует записать атрибуты до завершения span.

### Сохранённые контракты

Основа этого среза — upstream
[`7cb75fb2`](https://github.com/maximhq/bifrost/tree/7cb75fb2f8276ca48d0792c857fecb67b8879e2f).
Ниже перечислены сохранённые отличия и их проверяемые контракты относительно
этой базы. При обновлении upstream каждое отличие пересматривается отдельно.

| Область | Сохраняемый контракт | Исходники |
|---|---|---|
| Срок хранения кэша | Последовательная очистка просроченных записей; условное удаление не стирает обновлённую запись с тем же ID; неиндексируемые поля Qdrant не получают индекс | [vectorstore](framework/vectorstore/qdrant.go), [semanticcache](plugins/semanticcache/main.go) |
| Governance `Retry-After` | Время ожидания следует из того же окна, которое сбрасывает ограничивающий счётчик; неизвестный срок остаётся неизвестным | [governance](plugins/governance/store.go), [HTTP headers](transports/bifrost-http/lib/responseheaders.go) |
| Отдельный запуск локальных тестов | `-short` и перечисление тестов semantic cache не подключаются к внешним backend и не удаляют их namespace | [TestMain](plugins/semanticcache/main_test.go), [test helpers](plugins/semanticcache/test_utils_test.go) |
| Отмена | `Done`, `Err` и причина отмены согласованы; обычный дочерний context сохраняет собственный срок | [BifrostContext](core/schemas/context.go) |
| Доступность кэша | Недоступный кэш отключается до фонового восстановления; ограничение чтения store не сокращает срок embedding или самого запроса | [Qdrant](framework/vectorstore/qdrant.go), [semanticcache](plugins/semanticcache/main.go), [readiness](transports/bifrost-http/handlers/health.go) |
| Декодирование replay | Сохранённый raw response воспроизводится без округления JSON-чисел; правила сопоставления запросов остаются штатными | [cached response decoding](plugins/semanticcache/utils.go) |
| Повтор транспорта | Fasthttp не повторяет уже записанный неидемпотентный запрос скрыто внутри одной попытки; решение о следующей попытке остаётся в штатном retry/fallback loop | [transport](core/providers/utils/roundtripper.go), [retry loop](core/bifrost.go) |
| Каталог Routing Rules | `/v1/models` и SDK-каталоги включают доступные имена из живых правил рядом с прямыми моделями, с общей pagination; отдельный `/v1/routing/models` сохранён на время вывода старых клиентов | [routing](plugins/routing/models.go), [HTTP admission](transports/bifrost-http/server/routingmodels.go) |
| Фильтры журнала SQLite | Покрывающие индексы для моделей и ключей сохраняют штатные SQL, срок выборки и ограничения видимости; Dashboard запрашивает MCP-справочник только на вкладке MCP | [SQLite indexes](framework/logstore/sqlitefilters.go), [dashboard](ui/app/workspace/dashboard/page.tsx) |
| Вход OpenID Connect | Настройки Security и обычные dashboard sessions поддерживают OIDC без отдельного auth-сервиса; допускаются явно указанные subject или подтверждённые email | [настройка и сессии](docs/quickstart/gateway/oidc.mdx), [HTTP flow](transports/bifrost-http/handlers/oidc.go) |

Первый выпуск OIDC сохраняет работающий вход по локальному паролю. Версия
[`b3682eb10`](https://github.com/Labpics-Team/bifrost/commit/b3682eb10ec6a479b0139f33253aa03fcb2297bb),
предшествующая OIDC, не знает этот способ входа: возврат к ней после удаления
локальных credentials оставит панель без доступного входа. До появления и
проверки OIDC-совместимой версии для возврата локальные username/password
сохраняются в настройках и файле конфигурации; вход по ним проверяется в
отдельной браузерной сессии. Порядок перехода описан в
[инструкции OIDC](docs/quickstart/gateway/oidc.mdx#first-release-and-rollback).

Создание парольной или OIDC-сессии сверяет использованную политику доступа и
записывает сессию в одной транзакции. Эта операция и смена auth-настроек
блокируют одни и те же строки: устаревший вход не восстанавливает доступ после
отзыва, а сохранение неизменённой политики не сбрасывает сессии.

Причины проверяются по поведению. Например, upstream разрешает повтор после
записи на reused connection в
[roundtripper.go](https://github.com/maximhq/bifrost/blob/7cb75fb2f8276ca48d0792c857fecb67b8879e2f/core/providers/utils/roundtripper.go)
после потери ответа.
Ограничение скрытого транспортного повтора отделено от настроенной политики
повторов inference: после ошибки транспорта решение принимает штатный loop.

Старые `tool-output-identity`, `json-contract`, `embedding-space`,
`embedding-cache-ttl`, `request-outcome` и `toolchain` патчи не возвращаются.
Часть заменена upstream, часть удалена после пересмотра обязанностей.
Сохранение native ответа не означает проверку текста модели по собственной
JSON Schema. Прежняя собственная схема HTTP/SSE-наблюдаемости и глобальный
запрет повторов через `request_outcome_unknown` не используются.

## Границы поведения

Штатные `MaxRetries`, смена ключа, fallback и TTFT сохраняют семантику upstream.
После потери ответа или таймаута шлюз не всегда знает, выполнил ли provider
генерацию. Настроенная повторная попытка может повторить вычисление и расход;
единственное выполнение не гарантируется. Клиентские SDK могут иметь свой
бюджет повторов независимо от шлюза. Защита fasthttp ограничивает только
скрытые повторные отправки внутри одной попытки; `net/http` сохраняет штатное
поведение транспорта и redirects.

Provider `Retry-After` сохраняет штатный
[информационный смысл](docs/features/retries-and-fallbacks.mdx#provider-retry-hints),
parser и ограничение 1 секунда — 5 минут. Loop ждёт настроенный backoff и сохраняет
его штатную отмену. Неограниченное ожидание hint из прежнего патча отклонено:
таймаут HTTP-вызова не ограничивает паузу между попытками, а общий бюджет времени
не гарантирован. Новая политика ожидания не входит в этот перенос.
Исправление `Retry-After` для собственного governance-лимита остаётся: его значение
рассчитывается по фактическому окну ограничивающего счётчика.

Сопоставление кэша сохранено как у авторов upstream. Direct сравнивает хеши
нормализованного ввода и штатно извлечённых параметров. Текст приводится к нижнему
регистру, пробельные символы по краям удаляются; внутренние пробелы не схлопываются.
Авторская цель нормализации — «for better cache hit rates», как указано в
[commit ef07c60e](https://github.com/maximhq/bifrost/commit/ef07c60e3666523c468fb6ec9e40bf876bcbcb14).
В [PR #3210](https://github.com/maximhq/bifrost/pull/3210) поля, трактуемые как
множества, включая tools, stop sequences, modalities и include, сортируются для
уменьшения зависимости хеша от порядка элементов; tools сортируются по имени.
Извлечение параметров, hashing и правила поиска соответствуют закреплённой базе.
Direct-only отключает поиск по сходству embeddings, но сохраняет эту нормализацию.
Дополнительная HTTP identity, raw-body hash и особое ограничение raw passthrough
на direct replay удалены. Provider prompt caching остаётся независимым механизмом
и не изменён. Настройки описаны в [semantic caching](docs/features/semantic-caching.mdx).

У [Responses extractor закреплённого upstream](https://github.com/maximhq/bifrost/blob/7cb75fb2f8276ca48d0792c857fecb67b8879e2f/plugins/semanticcache/utils.go)
при проверке исходников отмечены следующие ограничения, сохраняющиеся в этом срезе:

- Структурный `tool_choice` учитывает только `name`; у `allowed_tools` без имени режим и список разрешённых tools не входят в ключ.
- `text.format` учитывает только `type`; JSON Schema и `strict` не входят в ключ. Различия только в этих пропущенных ограничениях могут дать одинаковую cache identity.
- Tools с одинаковыми или отсутствующими именами сохраняют исходный порядок при сортировке; их перестановка может создать дополнительный miss.

Это кандидаты для исправлений в upstream. Они отделены от сохраняемых исправлений
форка: доступности кэша, условной очистки и декодирования replay.

Просроченное попадание будит существующий последовательный worker условной
уборки. Между штатными минутными проходами допускается одна дополнительная
попытка; успешное восстановление namespace также разрешает такую попытку.
Текущий запрос продолжает обработку как cache miss. После уборки следующий
поиск может найти свежий результат; ошибка удаления оставляет штатный интервал
повторения. Так сбой хранилища не запускает очистку на каждый запрос.

`/readyz` проверяет необходимые config/log stores и считает VectorStore
необязательным для текущего использования в качестве кэша ответов. Перед
включением semantic routing, Warp или иной функции, которой store нужен для
выполнения запроса, нужно пересмотреть её зависимость и readiness.

Каталог Routing Rules публикует глобальные включённые terminal rules вида
`model == "имя"`. Он применяет native admission и права на выбираемые primary
targets, сохраняет приоритет первого объявления с пригодным target и pagination. Каталог не
предсказывает произвольную CEL-маршрутизацию и не обещает успех inference.
Общий `/v1/models` и `/openai/v1/models` дополняют этими именами штатный каталог
провайдеров. Изменение правила сразу меняет выдачу; отдельного реестра алиасов нет.
Сортировка и pagination выполняются после объединения, а явный запрос каталога
одного провайдера сохраняет его собственную выдачу. При отказе discovery
провайдеров разрешённые алиасы остаются в каталоге, когда источник отказа явно
обозначен на native discovery boundary; статусы ключей сохраняют диагностику.
Если discovery всех провайдеров завершился ошибкой, отказ политики или ошибка
неизвестного происхождения имеют приоритет над provider outage независимо от
порядка завершения. Частично успешная штатная выдача сохраняется.
Ошибки допуска, hooks, проверки direct key и отмена запроса сохраняются;
неизвестный источник ошибки не считается отказом провайдера.

Идентичность embeddings следует из фактических provider/model и размерности.
Смена весов или preprocessing под прежним model ID неразличима по стандартному
ответу: нужна новая идентичность либо переиндексация. Encoder для кэша выбирают
через Routing Rule до lookup; смена encoder требует нового namespace.

### Существующие vector namespaces

Semantic routing объявляет фильтруемым `kind`, Warp — поля своих search,
visibility и retention-фильтров. Qdrant сохраняет прежний payload; повторное
создание namespace добавляет недостающие индексы. Response payloads кэша
не индексируются.

Существующий фильтруемый integer-индекс должен поддерживать equality и range
операторы общего VectorStore API. Явные `lookup: false` или `range: false`
считаются несовместимой схемой; форк сообщает ошибку и сохраняет индекс без
пересоздания. Отсутствующие параметры используют штатные значения Qdrant.

В native Redis `Filterable` также включает hex-представление строковых TAG.
Поэтому complexity использует следующую версию fingerprint — `semantic-router-v2`:
штатный warmup создаёт новую генерацию и не принимает completion marker прежней.
При будущем переходе с уже заполненным Redis namespace Warp нужен
[штатный backfill](docs/features/warp.mdx#backfill) по сохраняемому
периоду с `restart: true`, чтобы перезаписать метаданные вместо продолжения
старого checkpoint.

В примере указан путь для размещения в корне. За reverse proxy с base path `/bifrost`
тот же запрос отправляется на `/bifrost/api/warp/log-index/backfill`.

```http
POST /api/warp/log-index/backfill
Content-Type: application/json

{
  "start_time": "2026-10-01T00:00:00Z",
  "end_time": "2026-10-08T00:00:00Z",
  "restart": true
}
```

Период выбирается по сохранённым исходным логам: удалённые или скрытые данные
не восстанавливаются из векторов. Для заполнения с нуля native Warp config
позволяет выбрать новый `log_vector_store_namespace` и выполнить тот же backfill.

## Разработка и проверка

### Адаптивная маршрутизация

Форк реализует документированный выбор провайдера и ключа внутри существующего
routing plugin; настройки, dashboard и recent incidents доступны в штатном
разделе Adaptive Routing. При отсутствии сохранённых настроек и блока
`load_balancer_config` путь существующего развёртывания не меняется. Это собственная
реализация публично описанного поведения, а не закрытой формулы Enterprise.
Состояние ограничено, причины решений идут в обычные request logs, в ConfigStore
сохраняются настройки, все неразрешённые ограничения и жизненный цикл инцидентов.
Статистика ограничена отдельно; её очистка не снимает ограничения. Смена ключа,
конфигурации провайдера и поздние ответы разделены поколениями, а SQLite фиксирует
переход до публикации его в маршрутизации. Штатные значения по умолчанию не считаются
сменой ключа при перезапуске. Состояние рассчитано на один активный процесс. Границы, включение, API и проверки
описаны в [справке adaptive routing](docs/features/adaptive-routing.mdx).

Все модули шлюза собираются из этого checkout. Go module paths upstream
сохранены для совместимости; принадлежность текущим исходникам проверяет
[ci/verify.py](ci/verify.py). На машине с Go и Python:

```sh
python3 ci/verify.py workspace
python3 ci/verify.py format
python3 ci/verify.py unit
python3 ci/verify.py test
```

`unit` выполняет полные short-наборы пакетов, изменённых относительно объявленной
базы upstream. Новые тесты и пакеты обнаруживаются автоматически через Git и Go.
Внешние интеграции соблюдают [testing.Short](https://pkg.go.dev/testing#Short);
их явный пропуск не считается проверкой этой интеграции.

`test` выполняет обязательные регрессии; отсутствующий, пропущенный или упавший
обязательный тест не считается успешной проверкой. Для Qdrant-регрессий нужен
изолированный тестовый Qdrant. `semanticcache` выполняет весь локальный корпус
с `-short`. CI задаёт fixture и toolchain в
[workflow](.github/workflows/labpics.yml). Ключи LLM-провайдеров не используются.
Этот workflow проверяет изменённые Go-пакеты и обязательные контракты. Он
не собирает и не публикует production-образ и не обновляет работающий сервис.

Для сборки локального приложения после подготовки workspace:

```sh
make build LOCAL=1
```

Команда собирает существующий UI и встраивает его в `tmp/bifrost-http`;
нужны Node из [`.nvmrc`](.nvmrc), Go и C toolchain для SQLite. Сам бинарный
entry point использует `go:embed all:ui`, поэтому Go-сборке приложения нужны
подготовленные UI assets. Проверки пакетов выше обходятся без сборки UI.

Обычный upstream `transports/Dockerfile` и `GOWORK=off` скачивают опубликованные
модули, поэтому не являются способом собрать этот форк. Локальную сборку
обеспечивают workspace и `LOCAL=1`; её результат не является опубликованным
образом или проверкой production runtime.

## Сборка и поставка

[Рецепт Infra](https://github.com/Labpics-Team/labpics-infra/blob/main/services/bifrost/build/Dockerfile)
получает полный commit форка из
[source.json](https://github.com/Labpics-Team/labpics-infra/blob/main/services/bifrost/build/source.json),
собирает UI и Go-модули из его исходников и проверяет native regressions и
production runtime перед публикацией. База upstream и обязательные тесты
принадлежат [ci/verification.json](ci/verification.json) этого форка:
Infra больше не применяет внешний набор патчей и не ведёт его копию тестов.
Рецепт переключён в [Infra #2683](https://github.com/Labpics-Team/labpics-infra/pull/2683).

[Workflow Infra](https://github.com/Labpics-Team/labpics-infra/blob/main/.github/workflows/build-bifrost.yml)
публикует проверенный образ в `ghcr.io/labpics-team/bifrost`, сохраняя
идентичность исходного коммита форка и ревизии сборки Infra.
Чтобы выпустить следующее изменение форка, нужно обновить pin в `source.json`
через PR в Infra, дождаться проверок и публикации, затем закрепить полученный
digest отдельным GitOps PR в
[декларации сервиса](https://github.com/Labpics-Team/labpics-infra/blob/main/services/bifrost/k8s/workload.yaml).
Первый переход на образ форка выполнен в
[Infra #2685](https://github.com/Labpics-Team/labpics-infra/pull/2685).

После применения GitOps проверяются фактический imageID запущенного контейнера,
готовность и авторизованный запрос через публичный путь. Успешный merge или
публикация образа не заменяют эту проверку. Runtime policy, ключи и данные
не хранятся в этом форке.

## Обновление upstream

`origin` — форк Labpics, `upstream` — `maximhq/bifrost`. Ветки `main` и `dev`,
скопированные GitHub при создании форка, не заменяют свежий `git fetch upstream`.
Обновление выбранного выпуска проходит PR в `labpics` вместе с обновлением базы
в `ci/verification.json` и проверкой перечисленных контрактов.

Если upstream реализует нужное поведение, локальная реализация удаляется,
а проверка поведения сохраняется. Обновление не должно возвращать удалённые
обходные решения, менять существующие данные или отключать регрессии ради
чистого merge. Изменение производительности измеряется на затронутом пути с
`-benchmem`; миграция тех же исходников не является новым ускорением.
