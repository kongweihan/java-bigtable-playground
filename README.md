# Screen command help
```
ctrl a+d: detach session
screen -ls: list sessions
screen -S session_name: create session
screen -r session_name: re-attach session
screen -rd session_name: detach then re-attach to a session
```

# Install common tools on debian Linux
```
sudo apt update
sudo apt install screen
sudo apt install default-jdk
javac -version
sudo apt install maven
mvn -version
sudo apt install git
git --version
```

# Run the Main benchmark
```
mvn compile exec:java -Dexec.mainClass=org.example.Main -Dexec.args="google.com:cloud-bigtable-dev kongwh-hsbc test-dg-hsbc 2000"
```