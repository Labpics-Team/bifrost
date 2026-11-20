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

Первый срез ограничен исходниками, документацией и проверками этого репозитория.
Infra продолжает собирать закреплённый upstream с прежними патчами. Переход
на исходники форка, публикация образа и обновление работающего сервиса требуют
отдельного поручения; merge в `labpics` запускает только проверки форка.

## Почему изменения сохранены

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
| Идентичность replay | Direct сохраняет точный ввод и порядок параметров; semantic допускает приблизительное сходство ввода при тех же параметрах; opaque JSON сохраняет числовую точность; raw passthrough получает только direct replay | [cache identity](plugins/semanticcache/cacheidentity.go), [hashing](plugins/semanticcache/utils.go) |
| Повтор транспорта | Fasthttp не повторяет уже записанный неидемпотентный запрос скрыто внутри одной попытки; решение о следующей попытке остаётся в штатном retry/fallback loop | [transport](core/providers/utils/roundtripper.go), [retry loop](core/bifrost.go) |
| Каталог Routing Rules | `/v1/routing/models` читает доступные caller публичные имена из живых правил без второго каталога и запросов к провайдерам | [routing](plugins/routing/models.go), [HTTP admission](transports/bifrost-http/server/routingmodels.go) |

Причины проверяются по поведению. Например, upstream сознательно нормализует ввод
direct-кэша ради частоты попаданий в [utils.go](https://github.com/maximhq/bifrost/blob/7cb75fb2f8276ca48d0792c857fecb67b8879e2f/plugins/semanticcache/utils.go),
разрешает повтор после записи на reused connection в
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

Direct replay различает регистр и пробелы: вопросы о `print('US'.isupper())`
и `print('us'.isupper())` требуют разных ответов. Локальная регрессия
`TestDirectCacheKeepsCaseSensitiveCodeDistinct` воспроизводит ошибочный `True`
вместо `False` на закреплённом upstream и проходит с точной идентичностью.
Прежние тесты нормализации описывали намеренное поведение upstream;
в форке это сознательное изменение границы replay. Semantic остаётся
приблизительным и нормализует текст для embeddings. Для кода, цитат и других
чувствительных к регистру задач нужен direct-only либо `x-bf-cache-type: direct`;
`threshold: 1` не превращает semantic в точное сравнение. Цена точного matching —
меньше попаданий для вариантов текста и перестановок параметров.
Настройки и границы описаны в [semantic caching](docs/features/semantic-caching.mdx).

Протокольные HTTP-заголовки и query входят в identity после применения всех
HTTP pre-hooks: custom plugin, расположенный после cache, может законно менять
эти параметры. Источник — штатные снимки итогового входящего запроса; это не
обещание снимка окончательных outbound headers провайдера.

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

## Отдельная поставка

Действующий [рецепт Infra](https://github.com/Labpics-Team/labpics-infra/tree/21660ec01806179dea062f54904c0e995678acc1/services/bifrost/build)
получает upstream `7cb75fb2`, применяет восемь патчей из своего `upstream.json`,
собирает UI и образ, выполняет native race-проверки и runtime smoke. Начальный
набор обязательных тестов форка сохранён из этого manifest. Перенос исходников
в этот репозиторий ещё не переключает рецепт Infra на форк.

После отдельного поручения рецепт Infra можно перевести на полный commit форка
и его `ci/verification.json`, заменив применение патчей. Тогда идентичность
исходников форка и ревизия рецепта сборки фиксируются отдельно, а уже собранный
образ проходит существующие native и runtime проверки перед публикацией.

Действующие образы публикует Infra в `ghcr.io/labpics-team/bifrost`.
Установленная версия закреплена по digest в
[декларации сервиса](https://github.com/Labpics-Team/labpics-infra/blob/main/services/bifrost/k8s/workload.yaml).
Обновление образа проходит обычный GitOps PR с проверкой фактического imageID
и запроса потребителя. Runtime policy, ключи и данные не хранятся в этом форке.

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
