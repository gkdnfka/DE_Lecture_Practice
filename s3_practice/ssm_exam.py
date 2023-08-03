import boto3


def put_parameter(name, value, value_type="SecureString", overwrite=True):
    ssm = boto3.client("ssm", region_name="ap-northeast-2")
    ssm.put_parameter(
        Name= name,
        Value = value,
        Type = value_type,
        Overwrite = overwrite
    )


def get_parameter(name):
    ssm = boto3.client("ssm", region_name="ap-northeast-2")
    parameter = ssm.get_parameter(Name=name, WithDecryption=True)

    return parameter["Parameter"]["Value"]


if __name__ == "__main__":
    #db_info = """{"host": "localhost", "port" : "3306"}"""
#    put_parameter("/dev/de4/mysql", db_info) #name(key) - 무조건 root로 시작
    db_info = get_parameter("/dev/de4/mysql")
    print(db_info)
