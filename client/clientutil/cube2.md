sudo npm install cube
sudo mkdir -p /usr/local/var/log/cube
➜  cube  sudo chmod -R 777 /usr/local/var/log/cube/ 
➜  cube  sudo node bin/collector.js 2>&1 >> /usr/local/var/log/cube/collector.log &
[1] 11271
➜  cube  sudo node bin/evaluator.js 2>&1 >> /usr/local/var/log/cube/evaluator.log &