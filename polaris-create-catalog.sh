#! /bin/bash

TOKEN=$(curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
 -H "Content-Type: application/x-www-form-urlencoded" \
 -d "grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL" \
 | grep -o '"access_token":"[^"]*' | grep -o '[^"]*$')


curl -i -X POST http://localhost:8181/api/management/v1/catalogs \
       -H "Authorization: Bearer $TOKEN" \
       -H "Content-Type: application/json" \
       -H "Polaris-Realm: POLARIS" \
       -d '{
         "catalog": {
           "name": "my_test_catalog",
           "type": "INTERNAL",
           "properties": {
            "default-base-location": "file:///tmp/polaris/"
          },
          "storageConfigInfo": {
            "storageType": "FILE",
            "allowedLocations": [
              "file:///tmp/polaris/"
            ]
          }
        }
      }'
