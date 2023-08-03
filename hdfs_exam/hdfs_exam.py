from hdfs import InsecureClient


host = "Server IP Address OR Domain Name:9870"
client = InsecureClient(host, user="hadoop")

target_dir = "data_directory"
paths = client.list(target_dir)
print(paths)
print(len(paths))

client.download(target_dir, "./data", overwrite=True)