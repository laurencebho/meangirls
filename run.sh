pyro4-ns &
python3 frontend.py &
for i in $(seq 3); do python3 replica.py & done

# run via command 'source run.sh'
# kill $(jobs -p)
