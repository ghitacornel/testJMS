list docker running containers
=> docker container ls

list all docker containers
=> docker container ls -a

remove docker container
=> docker container rm [container id or name]
example => docker container rm activeMQ 

stop docker container
=> docker stop [container id or name]
example => docker stop activeMQ 

start docker container
=> docker start [container id or name]
example => docker start activeMQ

create docker container
from docker-compose.yml => docker-compose up


from image => docker create --name [container name] [image name]
example => docker create --name activeMQ test_postgres