# Bullet Pulsar

This project implements Bullet PubSub through [Pulsar](https://pulsar.apache.org), allowing you to use Pulsar as your pubsub layer for transporting Bullet queries and results between the Bullet Web Service and Backend.

## Table of Contents

- [Background](#background)
- [Install](#install)
- [Usage](#usage)
- [Documentation](#documentation)
- [Links](#links)
- [Contribute](#contribute)
- [License](#license)

## Background

Bullet is a streaming query engine that can be plugged into any singular data stream using a Stream Processing framework like Apache [Storm](https://storm.apache.org), [Spark](https://spark.apache.org) or [Flink](https://flink.apache.org). It lets you run queries on this data stream - including hard queries like Count Distincts, Top K etc. The main project is available **[here](https://github.com/bullet-db/bullet-core)**.

## Install

Bullet Pulsar is a Java implementation of Bullet [PubSub](https://bullet-db.github.io/pubsub/architecture/), published to [Bintray](https://bintray.com/yahoo/maven/bullet-pulsar) and mirrored to [JCenter](http://jcenter.bintray.com/com/yahoo/bullet/bullet-pulsar/).  To see the various versions and set up your project for your package manager (Maven, Gradle etc), [see here](https://bullet-db.github.io/releases/#bullet-pulsar).

## Usage

For usage, please refer to **[Pulsar PubSub](https://bullet-db.github.io/pubsub/pulsar/)**.

## Documentation

All documentation is available at **[Github Pages here](https://bullet-db.github.io/)**.

## Links

* [PubSub Architecture](https://bullet-db.github.io/pubsub/architecture/) to see PubSub architecture.
* [Spark Quick Start](https://bullet-db.github.io/quick-start/spark) to start with a Bullet instance running locally on Spark.
* [Storm Quick Start](https://bullet-db.github.io/quick-start/storm) to start with a Bullet instance running locally on Storm.
* [Spark Architecture](https://bullet-db.github.io/backend/spark-architecture/) to see how Bullet is implemented on Storm.
* [Storm Architecture](https://bullet-db.github.io/backend/storm-architecture/) to see how Bullet is implemented on Storm.
* [Setup on Spark](https://bullet-db.github.io/backend/spark-setup/) to see how to setup Bullet on Spark.
* [Setup on Storm](https://bullet-db.github.io/backend/storm-setup/) to see how to setup Bullet on Storm.
* [API Examples](https://bullet-db.github.io/ws/examples/) to see what kind of queries you can run on Bullet.
* [Setup Web Service](https://bullet-db.github.io/ws/setup/) to setup the Bullet Web Service.
* [Setup UI](https://bullet-db.github.io/ui/setup/) to setup the Bullet UI.

## Contribute

All contributions are welcomed! Feel free to submit PRs for bug fixes, improvements or anything else you like! Submit issues, ask questions using Github issues as normal and we will classify it accordingly. See [Contributing](Contributing.md) for a more in-depth policy. We just ask you to respect our [Code of Conduct](Code-of-Conduct.md) while you're here.

## License

Code licensed under the Apache 2 license. See the [LICENSE](LICENSE) for terms.
