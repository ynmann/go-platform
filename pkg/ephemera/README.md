# ephemera

Sharded, generic, observable in-memory TTL-кэш на чистом stdlib.

Лок-страйпинг через `hash/maphash.Comparable`, singleflight loader для защиты
от cache stampede, atomic-метрики, pluggable `Clock`. Без внешних зависимостей.

---

## Возможности

- **Generic** по любому `comparable` ключу и любому value.
- **Lock striping** — N независимых шардов, никакого глобального мьютекса на горячем пути.
- **Singleflight loader** — `GetOrLoad` коалесцирует concurrent промахи в один вызов.
- **Per-entry TTL** через `SetWithTTL` поверх глобального default.
- **Atomic stats** — Hits / Misses / Sets / Evictions / Loads / LoadErrors + `HitRatio()`.
- **Pluggable Clock** — детерминированные тесты без `time.Sleep`.
- **Cooperative lifecycle** — `context.Context` + `*sync.WaitGroup`, плюс idempotent `Close()`.
- **OnEvict callback** — вызывается вне shard-локов (deadlock-safe).

---

## Quick start

```go
import (
    "context"
    "sync"
    "time"

    "git.pingocean.com/pasport/go-std/pkg/ephemera"
    "github.com/google/uuid"
)

ctx, cancel := context.WithCancel(context.Background())
defer cancel()
var wg sync.WaitGroup

cache := ephemera.New[uuid.UUID, User](ctx, &wg,
    ephemera.WithTTL(15*time.Minute),
    ephemera.WithShards(32),
    ephemera.WithCapacity(100_000),
)
defer cache.Close()

cache.OnEvict(func(id uuid.UUID, u User) {
    metrics.UserCacheEvictions.Inc()
})

user, err := cache.GetOrLoad(ctx, id, func(ctx context.Context, k uuid.UUID) (User, error) {
    return repo.FindUserById(ctx, k)
})
```

---

## Конструктор

```go
func New[K comparable, V any](
    ctx context.Context,
    wg  *sync.WaitGroup,
    opts ...Option,
) *Cache[K, V]
```

`New` запускает фоновую горутину для периодического sweep'а истёкших записей.
Горутина завершается, когда отменён `ctx` *или* вызван `Close()`; завершение
сигналится через `wg`.

---

## Опции

| Опция | Default | Описание |
|---|---|---|
| `WithTTL(d)` | `5m` | TTL для записей через `Set`. |
| `WithCleanupInterval(d)` | `1m` | Периодичность фонового sweep'а. |
| `WithShards(n)` | `16` | Число шардов; округляется вверх до степени двойки. |
| `WithCapacity(n)` | `0` | Подсказка для предалокации; распределяется по шардам. |
| `WithClock(c)` | `systemClock` | Источник времени; используйте моки в тестах. |

---

## API

### Чтение

```go
v, ok := cache.Get(k)               // не двигает TTL
v, ok := cache.GetAndTouch(k)       // продлевает TTL до default
ok     := cache.Touch(k)            // только продлить TTL
v, err := cache.GetOrLoad(ctx, k, loader) // singleflight loader
```

### Запись

```go
cache.Set(k, v)                     // default TTL
cache.SetWithTTL(k, v, 30*time.Second)
ok := cache.Update(k, func(old V) V { ... })
```

### Поиск и массовые операции

```go
v, ok := cache.FindAndTouch(func(k K, v V) bool { ... })
k, v, ok := cache.UpdateWhere(
    func(k K, v V) bool { ... },
    func(old V) V { ... },
)

cache.Range(func(k K, v V) bool {
    // вернуть false, чтобы остановиться
    return true
})
```

> `Range`/`FindAndTouch` итерируют по шардам в неопределённом порядке.
> Внутри `Range` нельзя вызывать методы того же `Cache` — будет deadlock на shard-локе.

### Lifecycle

```go
cache.Delete(k)   // OnEvict не вызывается для explicit-удалений
cache.Purge()     // очистить всё, без OnEvict
cache.Close()     // остановить sweep-горутину, идемпотентно
```

### OnEvict

```go
cache.OnEvict(func(k K, v V) {
    log.Info("evicted", "key", k)
})
```

Вызывается ровно один раз на запись, удалённую периодическим sweep'ом.
Не вызывается для `Delete`, `Purge`, или при отмене `ctx`.
Колбэк выполняется вне shard-локов — внутри можно безопасно дёрнуть кэш.

---

## Observability

```go
s := cache.Stats()
// s.Hits, s.Misses, s.Sets, s.Evictions, s.Loads, s.LoadErrors, s.Size
log.Info("cache", "hit_ratio", s.HitRatio(), "size", s.Size)
```

`Size` — точный счёт живых (не истёкших) записей; стоит O(N) — не вызывайте на горячем пути.
Остальные счётчики — atomic, дёшевы.

---

## Singleflight loader

`GetOrLoad` решает классическую проблему cache stampede: при одновременном
промахе по одному ключу `loader` будет вызван **ровно один раз**, остальные
caller'ы получат тот же результат.

```go
v, err := cache.GetOrLoad(ctx, key, func(ctx context.Context, k Key) (Value, error) {
    return slowDownstream.Fetch(ctx, k)
})
```

При ошибке loader'а ничего не кэшируется — следующий вызов попробует снова.

---

## Тестирование с кастомным Clock

```go
type fakeClock struct{ now time.Time }
func (c *fakeClock) Now() time.Time { return c.now }
func (c *fakeClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

clk := &fakeClock{now: time.Unix(0, 0)}
cache := ephemera.New[string, int](ctx, &wg,
    ephemera.WithTTL(time.Minute),
    ephemera.WithClock(clk),
)
cache.Set("k", 42)
clk.Advance(2 * time.Minute)
_, ok := cache.Get("k") // false: истекло
```

---

## Модель конкурентности

- Все публичные методы безопасны для concurrent-вызова.
- Записи из разных шардов не блокируют друг друга.
- `Get` берёт RLock, `Set/Delete/Update` — Lock.
- `Range` держит RLock по шарду на время вызова `fn`. Долгие `fn` блокируют writes в этот шард.
- `OnEvict` вызывается вне shard-локов, безопасно дёргать кэш изнутри.

---

## Требования

- Go ≥ 1.24 (`hash/maphash.Comparable`).
- Только stdlib.
