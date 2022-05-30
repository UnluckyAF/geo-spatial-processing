# Build

```mvn clean compile assembly:single```

# Run

    java -cp target/cloud_setup-1.0.0-jar-with-dependencies.jar setup.App -a "setup"
    java -cp target/cloud_setup-1.0.0-jar-with-dependencies.jar setup.App -a "create"
    java -cp target/cloud_setup-1.0.0-jar-with-dependencies.jar setup.App -a "delete" -network_id <id>

    java -cp target/cloud_setup-1.0.0-jar-with-dependencies.jar setup.App -a "setup" -v
    ssh -i ~/.ssh/cloud barukhov@<public_ip>

# On vm
    sudo apt-get update && sudo apt-get upgrade
    sudo apt-get install openjdk-8-jdk
    sudo mkdir /usr/local/spark && sudo chmod 777 /usr/local/spark
    sudo scp  -i ~/.ssh/cloud -r /usr/local/best_spark barukhov@<ip>:/usr/local/spark
    export SPARK_MASTER_HOST=<internal_ip>
in app use public_ip

