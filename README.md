# java-kafka-python
Integrating Java and Python via Kafka


## Requirements

- OS: WSL with Arch Linux, clone repo:

```shell
cd ~/dev
git clone https://github.com/reisarthur/java-kafka-python.git
```

- Kafka 3.7.0 - run on docker:

```shell
cd ~/dev/java-kafka-python/kafka/
docker compose up
```

- Python 3.11 - create virtual environment, and install module kafka-python:

```shell
cd ~/dev/java-kafka-python/python/
python -m venv myenv
source myenv/bin/activate.fish
pip install kafka kafka-python
```


## Usage

Run on three different tabs:

```shell
cd ~/dev/java-kafka-python/python/
python analysis-manager.py
```

```shell
cd ~/dev/java-kafka-python/java/analysis-requester
mvn exec:java -Dexec.mainClass="br.cepel.dga.AnalysisListener"
```

```shell
cd ~/dev/java-kafka-python/java/analysis-requester
mvn exec:java -Dexec.mainClass="br.cepel.dga.AnalysisRequester"
```
