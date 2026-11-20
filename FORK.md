# Bifrost в Labpics

Этот репозиторий — поддерживаемый форк [maximhq/bifrost](https://github.com/maximhq/bifrost).
Рабочая ветка — `labpics`. Исходная лицензия [Apache 2.0](LICENSE) сохранена.
База upstream и обязательные регрессии объявлены в [ci/verification.json](ci/verification.json).
Исходники core, framework, plugins и HTTP transport изменяются здесь обычными PR.

Первый перенос сохраняет поведение сборки из
[Labpics Infra `21660ec01`](https://github.com/Labpics-Team/labpics-infra/tree/21660ec01806179dea062f54904c0e995678acc1/services/bifrost/build):
восемь действующих патчей стали исходниками вместе с их тестами. Дерево после
применения прежнего набора — `30ab5f078c34c03eab90c32dce56a727a1060161`.
Сам перенос не добавляет работы или выделений памяти на пути запроса.

## Почему изменения сохранены

Сверка 20 ноября 2026 с upstream
[`main` на `155c2f4`](https://github.com/maximhq/bifrost/tree/155c2f4db4da9c7c30d840765c17bf46936215e1)
не обнаружила равнозначной замены следующего поведения.

| Область | Сохраняемый контракт | Исходники |
|---|---|---|
| Срок хранения кэша | Последовательная очистка просроченных записей; условное удаление не стирает обновлённую запись с тем же ID; неиндексируемые поля Qdrant не получают индекс | [vectorstore](framework/vectorstore/qdrant.go), [semanticcache](plugins/semanticcache/main.go) |
| Governance `Retry-After` | Время ожидания следует из того же окна, которое сбрасывает ограничивающий счётчик; неизвестный срок остаётся неизвестным | [governance](plugins/governance/store.go), [HTTP headers](transports/bifrost-http/lib/responseheaders.go) |
| Отдельный запуск локальных тестов | `-short` и перечисление тестов semantic cache не подключаются к внешним backend и не удаляют их namespace | [TestMain](plugins/semanticcache/main_test.go), [test helpers](plugins/semanticcache/test_utils_test.go) |
| Отмена | `Done`, `Err` и причина отмены согласованы; обычный дочерний context сохраняет собственный срок | [BifrostContext](core/schemas/context.go) |
| Доступность кэша | Недоступный кэш отключается до фонового восстановления; ограничение чтения store не сокращает срок embedding или самого запроса | [Qdrant](framework/vectorstore/qdrant.go), [semanticcache](plugins/semanticcache/main.go), [readiness](transports/bifrost-http/handlers/health.go) |
| Идентичность replay | Точный ввод, полные native-параметры и версия протокола различаются; opaque JSON сохраняет числовую точность; raw passthrough получает только exact replay | [cache identity](plugins/semanticcache/cacheidentity.go), [hashing](plugins/semanticcache/utils.go) |
| Повтор inference | Неизвестный исход отправленной генерации запрещает повтор, смену ключа и fallback; доказанный отказ допускает штатный повтор с provider `Retry-After` и отменой | [request outcome](core/providers/utils/requestoutcome.go), [retry loop](core/bifrost.go) |
| Каталог Routing Rules | `/v1/routing/models` читает доступные caller публичные имена из живых правил без второго каталога и запросов к провайдерам | [routing](plugins/routing/models.go), [HTTP admission](transports/bifrost-http/server/routingmodels.go) |

Причины проверяются по поведению. Например, upstream всё ещё нормализует ввод
точного кэша в [utils.go](https://github.com/maximhq/bifrost/blob/155c2f4db4da9c7c30d840765c17bf46936215e1/plugins/semanticcache/utils.go),
разрешает повтор после записи на reused connection в
[roundtripper.go](https://github.com/maximhq/bifrost/blob/155c2f4db4da9c7c30d840765c17bf46936215e1/core/providers/utils/roundtripper.go)
и описывает provider retry hint как
[информационный](https://github.com/maximhq/bifrost/blob/155c2f4db4da9c7c30d840765c17bf46936215e1/docs/features/retries-and-fallbacks.mdx).
По [RFC 9110 §9.2.2](https://www.rfc-editor.org/rfc/rfc9110.html#section-9.2.2)
потеря ответа сама по себе не разрешает повтор неидемпотентного запроса.

Старые `tool-output-identity`, `json-contract`, `embedding-space`,
`embedding-cache-ttl`, `request-outcome` и `toolchain` патчи не возвращаются.
Часть заменена upstream, часть удалена после пересмотра обязанностей.
Сохранение native ответа не означает проверку текста модели по собственной
JSON Schema. Активный `request_outcome_unknown` относится к безопасному повтору
inference; прежняя собственная схема HTTP/SSE-наблюдаемости удалена.

## Границы поведения

Provider `Retry-After` ограничивает повтор внутри одного provider loop.
Это не общий cooldown между fallback-маршрутами или независимыми запросами.

`/readyz` проверяет необходимые config/log stores и считает VectorStore
необязательным для текущего использования в качестве кэша ответов. Перед
включением semantic routing, Warp или иной функции, которой store нужен для
выполнения запроса, нужно пересмотреть её зависимость и readiness.

Каталог Routing Rules публикует глобальные включённые terminal rules вида
`model == "имя"`. Он применяет native admission и права на выбираемые primary
targets, сохраняет порядок первого объявления имени и pagination. Каталог не
предсказывает произвольную CEL-маршрутизацию и не обещает успех inference.

Идентичность embeddings следует из фактических provider/model и размерности.
Смена весов или preprocessing под прежним model ID неразличима по стандартному
ответу: нужна новая идентичность либо переиндексация. Encoder для кэша выбирают
через Routing Rule до lookup; смена encoder требует нового namespace.

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
Этот workflow проверяет изменённые Go-пакеты и обязательные контракты; сборка UI
и production-образа с runtime smoke выполняется в Infra при обновлении source pin.

Для локального приложения после подготовки workspace:

```sh
make run LOCAL=1
```

Обычный upstream `transports/Dockerfile` и `GOWORK=off` скачивают опубликованные
модули, поэтому не являются способом собрать этот форк. Production recipe,
закреплённые compiler/runtime images, race-проверки и smoke собранного образа
пока находятся в
[Infra build](https://github.com/Labpics-Team/labpics-infra/tree/main/services/bifrost/build).
Он получает полный commit форка, использует этот же workspace и manifest тестов.
Новых `.patch` для приложения в Infra нет.

Образ публикуется в `ghcr.io/labpics-team/bifrost`. Идентичность исходников форка
и ревизия рецепта сборки фиксируются отдельно; установленная версия закреплена
по digest в
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

## Дальнейшее развитие

Форк создаёт основу для следующих отдельных решений:

- объединение с AI Node с сохранением публичных протоколов, авторизации,
  приватности, отмены и границы безопасного повтора;
- оптимизация по измерениям latency, памяти и полезной пропускной способности;
- необходимые Labpics Enterprise-возможности поверх открытого кода;
- массовый импорт, ротация и отключение ключей с предварительным просмотром,
  проверкой прав, частичных ошибок, повторяемости операции и журналом изменений.

Эти направления требуют самостоятельной оценки существующих API и полезного
пользовательского сценария. Их наличие в этом списке не означает, что они уже
реализованы или включены в production.
