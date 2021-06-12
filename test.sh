for i in {1..100}; do 
    curl --request PUT --header "Content-Type: application/json" --write-out "\n%{http_code}\n" --data '{"value": "skdh" }'  127.0.0.1:8082/key-value-store/foo${i}
done