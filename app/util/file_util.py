import json

def json_loader(filename):
  try:
    f = open(filename,'r')
    json_string = f.read()
    json_dict = json.loads(json_string)

    return json_dict

  except Exception as error:
    raise Exception(error)
  
  finally:
    f.close()