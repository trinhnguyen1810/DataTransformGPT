NUM_WORKERS=10

for ((i=1; i<=NUM_WORKERS; i++))
do
    python -m src.workers.worker &
    echo "Started worker $i"
done