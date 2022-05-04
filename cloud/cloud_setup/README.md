# Build

```mvn clean compile assembly:single```

# Run

    java -cp target/cloud_setup-1.0.0-jar-with-dependencies.jar setup.App -a "setup"
    java -cp target/cloud_setup-1.0.0-jar-with-dependencies.jar setup.App -a "create"
    java -cp target/cloud_setup-1.0.0-jar-with-dependencies.jar setup.App -a "delete" -network_id <id>

    java -cp target/cloud_setup-1.0.0-jar-with-dependencies.jar setup.App -a "setup" -v
    ssh -i ~/.ssh/cloud barukhov@<public_ip>
