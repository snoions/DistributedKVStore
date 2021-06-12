subnetName="assignment4-net"
subnetAddress="10.10.0.0/16"
nodeIpList=("10.10.0.2" "10.10.0.3" "10.10.0.4" "10.10.0.5" "10.10.0.6" "10.10.0.7")
nodeHostPortList=("8082" "8083" "8084" "8086" "8087" "8088")
nodeView=""
for i in ${!nodeIpList[@]};do
    nodeView="${nodeView},${nodeIpList[$i]}:8085"
done
nodeView=${nodeView:1}
docker network create --subnet ${subnetAddress} ${subnetName}
docker build . -t assignment4-img
for i in ${!nodeIpList[@]};do
    docker run  --detach\
                --publish ${nodeHostPortList[$i]}:8085 \
                --net=${subnetName} \
                --ip=${nodeIpList[$i]} \
                --name=node${i} \
                -e SOCKET_ADDRESS=${nodeIpList[$i]}:8085 \
                -e VIEW=${nodeView[@]}\
                -e SHARD_COUNT=2\
                assignment4-img
done

# sleep 5
# for i in {1..600}; do 
#     port=${nodeHostPortList[${i}%6]}
#     curl --request PUT --header "Content-Type: application/json" --write-out "\n%{http_code}\n" --data '{"value": "value" }'  127.0.0.1:${port}/key-value-store/key${i}
# done
# sleep 5
# for i in {1..600}; do 
#     port=${nodeHostPortList[${i}%6]}
#     curl --request GET --header "Content-Type: application/json" --write-out "\n%{http_code}\n" 127.0.0.1:${port}/key-value-store/key${i}
# done
