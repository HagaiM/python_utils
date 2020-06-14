import json,ast
from pandas.io.json import json_normalize

data_v1 = '''
[{
    "organization": "nation1",
    "job_id": 1,
    "job_name": "job1",
    "state": {
        "started": "no"
    },
    "timetamp": 1570357814930
},
{
    "organization": "nation1",
    "job_id": 1,
    "job_name": "job1",
    "state": {
        "started": "yes",
        "attended": "yes"
    },
    "timetamp": 1570357814988
}]
'''


data_v2 = '''{ "trans" :
[{
    "organization": "nation1",
    "job_id": 1,
    "job_name": "job1",
    "state": {
        "started": "no"
    },
    "timetamp": 1570357814930
},
{
    "organization": "nation1",
    "job_id": 1,
    "job_name": "job1",
    "state": {
        "started": "yes",
        "attended": "yes"
    },
    "timetamp": 1570357814988
}]
}
'''

data_v3 = '''{ "trans" :
[{
    "organization": "nation1",
    "job_id": 1,
    "job_name": "job1",
    "state": {
        "started": "no"
    },
    "timetamp": 1570357814930
},
{
    "organization": "nation1",
    "job_id": 1,
    "job_name": "job1",
    "state": {
        "started": "yes",
        "attended": "yes"
    },
    "timetamp": 1570357814988
}],
 "transsss" :
[{
    "organization": "nnnn",
    "job_id": 1,
    "job_name": "job1",
    "state": {
        "started": "no"
    },
    "timetamp": 1570357814930
},
{
    "organization": "nation1",
    "job_id": 1,
    "job_name": "job1",
    "state": {
        "started": "yes",
        "attended": "yes"
    },
    "timetamp": 1570357814988
}]
}
'''

def json_to_df(data):
    flag = 0
    try:
        if isinstance(data, str):
            data = json.loads(data)

    except:
        pass
    if flag == 0:
        jsonObject = json.loads(json.dumps(data))

    else:
        jsonObject = data

    try:
        if isinstance(data, str):
            jsonObject = ast.literal_eval(jsonObject)
            data = ast.literal_eval(data)
        counter = 0
        result_set = {}
        for key in jsonObject:
            data_key = data[key]
            data_key = json.dumps(data_key)
            data_key = json_normalize(json.loads(data_key))
            result_set[counter] = data_key
            counter += 1
        return result_set
    except:
        try:
            data = json_normalize(data)
            return data
        except:
            data = json_normalize(json.loads(jsonObject))
            return data


print("------------------------------------------------------------------------------------")
print(json_to_df(data_v1))


print("------------------------------------------------------------------------------------")
value = json_to_df(data_v2)
for v in value:
    print(value[v])


print("------------------------------------------------------------------------------------")
value = json_to_df(data_v3)
for v in value:
    print(value[v])