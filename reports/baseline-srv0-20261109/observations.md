# Наблюдения при disk alarm

Эти completed RabbitMQ FAST runs исключены из основной агрегации: broker log сообщает о disk alarm и блокировке publishers. Это данные о поведении при заполнении диска, а не оценка предела брокера.

| Run ID | Клиенты | Offered, сообщ./с | Delivered, копий/с | Потери | Ошибки операций | Queue drops |
|---|---:|---:|---:|---:|---:|---:|
| 20261004T105138Z-rabbitmq-nir1_final_connection_density-9816cbf53039ab88 | 1000 | 100.000 | 65.030 | 10491 | 0 | 0 |
| 20261004T105927Z-rabbitmq-nir1_final_connection_density-29091042db94c209 | 1000 | 97.733 | 0.000 | 29320 | 86 | 680 |
| 20261004T110715Z-rabbitmq-nir1_final_connection_density-79c0fa26dd3556f5 | 1000 | 97.737 | 0.000 | 29321 | 43 | 679 |
| 20261004T111504Z-rabbitmq-nir1_final_connection_density-9e933cb850e2aeee | 5000 | 97.737 | 0.000 | 29321 | 39 | 679 |
| 20261004T112301Z-rabbitmq-nir1_final_connection_density-6fb6b797e2629dfb | 5000 | 97.743 | 0.000 | 29323 | 24 | 677 |
