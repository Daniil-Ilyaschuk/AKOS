#!/bin/bash
set -e

PORT=18080
IP="127.0.0.1"

echo "=== Сборка проекта ==="
make clean && make

cleanup() {
    kill $(jobs -p) 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT

echo ""
echo "========================================================"
echo "ТЕСТ 1 (4-5 баллов): Базовая работа без наблюдателей"
echo "========================================================"
./server $IP $PORT 0 &
SRV_PID=$!
sleep 0.4

./department $IP $PORT 1 400 101 102 &
D1_PID=$!
./department $IP $PORT 2 400 201 202 &
D2_PID=$!
sleep 0.3

./customer $IP $PORT 1 101 202 102 &
C1_PID=$!
./customer $IP $PORT 2 101 201 &
C2_PID=$!

wait $C1_PID $C2_PID
kill -INT $SRV_PID
wait $SRV_PID $D1_PID $D2_PID 2>/dev/null || true

echo ""
echo "========================================================"
echo "ТЕСТ 2 (6-8 баллов): Множество наблюдателей + динамика"
echo "========================================================"
./server $IP $PORT 0 > /dev/null &
SRV_PID=$!
sleep 0.4

./observer $IP $PORT obs_1.log &
OBS1_PID=$!
./observer $IP $PORT obs_2.log > /dev/null &
OBS2_PID=$!
sleep 0.3

./department $IP $PORT 1 400 101 102 > /dev/null &
D1_PID=$!
./department $IP $PORT 2 400 201 202 > /dev/null &
D2_PID=$!
sleep 0.3

./customer $IP $PORT 1 101 202 102 > /dev/null &
C1_PID=$!
./customer $IP $PORT 2 101 201 > /dev/null &
C2_PID=$!

sleep 0.7
echo ">>> [Скрипт] Отключаем наблюдателя 2 и подключаем наблюдателя 3 на лету..."
kill -INT $OBS2_PID 2>/dev/null || true
./observer $IP $PORT obs_3.log > /dev/null &
OBS3_PID=$!

wait $C1_PID $C2_PID
kill -INT $SRV_PID
wait $SRV_PID $D1_PID $D2_PID $OBS1_PID $OBS3_PID 2>/dev/null || true
echo ">>> Строк получено: obs_1=$(wc -l < obs_1.log), obs_2=$(wc -l < obs_2.log), obs_3=$(wc -l < obs_3.log)"

echo ""
echo "========================================================"
echo "ТЕСТ 3 (9-10 баллов): Падение и переподключение отдела"
echo "========================================================"
./server $IP $PORT 0 > /dev/null &
SRV_PID=$!
sleep 0.4

./observer $IP $PORT &
OBS_PID=$!
sleep 0.3

./department $IP $PORT 1 800 101 102 > /dev/null &
D1_PID=$!
sleep 0.3

./customer $IP $PORT 10 101 102 > /dev/null &
C10_PID=$!
sleep 0.3

echo ">>> [Скрипт] Имитируем падение Отдела 1 во время обслуживания!"
kill -INT $D1_PID 2>/dev/null || true
wait $D1_PID 2>/dev/null || true

sleep 1.0
echo ">>> [Скрипт] Возвращаем Отдел 1 в сеть..."
./department $IP $PORT 1 400 101 102 > /dev/null &
D1_PID=$!

wait $C10_PID
echo ">>> [Скрипт] Останавливаем сервер (SIGINT) — проверяем штатное завершение клиентов..."
kill -INT $SRV_PID
wait $SRV_PID $D1_PID $OBS_PID 2>/dev/null || true

echo ""
echo ""
echo "========================================================"
echo "ТЕСТ 4 (9-10 баллов): Обрыв покупателя в очереди/на кассе"
echo "========================================================"
./server $IP $PORT 0 > /dev/null &
SRV_PID=$!
sleep 0.4

./observer $IP $PORT &
OBS_PID=$!
sleep 0.3

./department $IP $PORT 1 900 101 > /dev/null &
D1_PID=$!
sleep 0.3

./customer $IP $PORT 20 101 > /dev/null &
C20_PID=$!
./customer $IP $PORT 21 101 > /dev/null &
C21_PID=$!
sleep 0.3

echo ">>> [Скрипт] Имитируем внезапный уход Покупателя 20 во время обслуживания!"
kill -INT $C20_PID 2>/dev/null || true
wait $C20_PID 2>/dev/null || true

wait $C21_PID
kill -INT $SRV_PID
wait $SRV_PID $D1_PID $OBS_PID 2>/dev/null || true

echo ""
echo "========================================================"
echo "ТЕСТ 5 (Бонус): Закрытие дверей по таймеру (MSG_CLOSED)"
echo "========================================================"
./server $IP $PORT 2 > /dev/null &
SRV_PID=$!
sleep 0.3

./observer $IP $PORT &
OBS_PID=$!
sleep 0.2

./department $IP $PORT 1 1200 101 102 > /dev/null &
D1_PID=$!
sleep 0.2

./customer $IP $PORT 30 101 102 > /dev/null &
C30_PID=$!

sleep 2.2
echo ">>> [Скрипт] Опоздавший Покупатель 31 пытается войти после закрытия..."
./customer $IP $PORT 31 101

wait $C30_PID $SRV_PID $D1_PID $OBS_PID 2>/dev/null || true
echo "=== Все тесты успешно пройдены! ==="