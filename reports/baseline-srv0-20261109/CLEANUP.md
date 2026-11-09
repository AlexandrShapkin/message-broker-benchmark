# Очистка srv0

На момент исходного commit публикации серверные данные ещё сохранены.
Удаление разрешено только после проверки удалённого commit, опубликованного
Release и скачанного набора по SHA-256 и полному файловому inventory.
Фактический результат будет записан последующим commit после очистки.

Подтверждённые ресурсы: `/home/shapkin_as/message-broker-benchmark`,
`/home/shapkin_as/benchmark-plan` и четыре неиспользуемых pinned broker images.
Контейнеров и volumes нет; сети только стандартные Docker bridge/host/none.
Benchmark процессов, сервисов и scheduled jobs при аудите не обнаружено.
Общие Docker/Go/Python/Node/Rust tools, кэши и unrelated services не удаляются.
