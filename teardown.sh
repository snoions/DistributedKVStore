docker kill `docker ps -q --filter ancestor=assignment4-img`
docker rm `docker ps -qa --filter ancestor=assignment4-img `
docker rmi assignment4-img
docker network rm assignment4-net
