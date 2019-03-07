pyro4-ns &
python frontend.py &
for i in $(seq 3); do python replica.py & done

# run via command 'source run.sh'
# kill $(jobs -p)
