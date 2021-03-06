# UID2 OptOut

The UID 2 Project is subject to Tech Lab IPR’s Policy and is managed by the IAB Tech Lab Addressability Working Group and Privacy & Rearc Commit Group. Please review the governance rules [here](https://github.com/IABTechLab/uid2-core/blob/master/Software%20Development%20and%20Release%20Procedures.md)

## Prerequisite

To setup dependencies before building, run the follow script

```bash
./setup_dependencies.sh
```

## Building

To run unit tests:

```
mvn clean test
```

To package application:

```
mvn package
```

To run application:

- use `conf/local-config.json` to run standalone optout service 
  for local debugging, which doesn't communicate with uid2-core.

```
mvn clean compile exec:java -Dvertx-config-path=conf/local-config.json
```

- use `conf/integ-config.json` to run optout service that
  integrates with uid2-core, which default runs on `localhost:8088`
  
```
mvn clean compile exec:java -Dvertx-config-path=conf/integ-config.json
```

