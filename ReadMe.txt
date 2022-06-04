To run this application for United Kigndom dag, follow the next steps:
1- make these directories 
 mkdir ./dags ./logs ./plugins ./data ./output
2- echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
3- init the airflow 
 docker-compose up airflow-init
4- get the docker compose up
 docker-compose up -d
5- Choose a country in a public function where it will get the resultes for it
6- now you can find the png in output file and the CAVs in the data file 
7- docker-compose down 