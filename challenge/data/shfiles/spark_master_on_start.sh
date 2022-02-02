echo "DEV INFO Install Python Requirements ..."
pip install -r ./data/pyfiles/requirements.txt
echo "DEV INFO Finish Install Python Requirements ..."

echo "DEV INFO Starting Python Scripts ..."
python -u ./data/pyfiles/data_engineer_pipe.py