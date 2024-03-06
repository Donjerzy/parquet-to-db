# Usage Instructions

1. Install Docker
2. Copy parquet file to parquet directory
3. Build Docker image
4. Create a docker container off of the image. Make sure to pass arguments.


# Arguments Documentation

parser = argparse.ArgumentParser(description='Parquet to Postgres Arguments')      
parser.add_argument('--user', help='Database user')                                
parser.add_argument('--password', help='Database password')                        
parser.add_argument('--host', help='Database host')                                
parser.add_argument('--port', help='Database port')                                
parser.add_argument('--database', help='Database name')                            
parser.add_argument('--file', help='Parquet file name')                            
parser.add_argument('--exists', help='Database table exists boolean')              
parser.add_argument('--table', help='Database table name')                         
parser.add_argument('--schema', help='Database schema')                            

# Sample Docker Run Command
docker run -it --network=pg-network parquet-to-postgres:001  --user=root  --password=root  --host=taxi_db  --port=5432  --database=data_eng  --file=test.parquet  --exists=False  --table=demo_1  --schema=public










  