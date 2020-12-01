echo clone github project repo
git clone https://github.com/ktochylin/big_data_procamp.git
sleep 1
cd ~/big_data_procamp/
sleep 1
git fetch origin
git branch -av
sleep 1
echo switch to test branch
git checkout -t origin/kafka-consumer-test
git branch
ls -la
sleep 1
cd labs/kafka/homework
ls -la
echo git project is ready
sleep 1
#cd ~
#sudo rm big_data_procamp/ --recursive

echo setup pyhon 3 virtual environment
sleep 1
mkdir venv && cd venv
sleep 1
virtualenv --python=python3 my_test_env
sleep 1
#ls my_env_3/lib
echo activate virtual environment
sleep 1
source my_test_env/bin/activate
echo pip install confluent-kafka
sleep 1
pip install confluent-kafka
sleep 1
python --version
sleep 1
cd ..
ls -la


sleep 1
echo clean up environment from project sources
sleep 1
deactivate
cd ~
sudo rm big_data_procamp/ --recursive
