## manticore-pusher
- build pusher like that `docker build -t manticore-pusher .`
- run compose `docker-compose run --service-ports pusher -w 10 -i 100000 -b 100 -c "Server=manticore;Port=9306;SslMode=None;"` where `-w` is table/connection count `-i` iteration count and `-b` batch size  