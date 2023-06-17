# Paris bikes availability map

## Description

This project aims to display a map of Paris with all the bikes station of the Paris city with the amount of bikes and docks available. All data are coming from the website of the bikes service of paris : [velib data](https://www.velib-metropole.fr/donnees-open-data-gbfs-du-service-velib-metropole).

Since the data of these station are not updated all together, I've chosen to refresh the ETL DAG (AirFlow) every hour. 



## How to use

All components of the application are containerized, and the only command to launch the application is : 

```
docker-compose up
```

You will need a docker engine to launch it.

The UI is accessible with : [UI](0.0.0.0:8050).
And you can change the parameters of the DAGs directly through the airflow dashboard : [airflow-ui](0.0.0.0:8080).






