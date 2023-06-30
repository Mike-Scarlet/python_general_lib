
import json
import typing

class IJsonSerializable:
  def ToJson(self) -> typing.Union[dict, list]:
    raise NotImplementedError()

  def FromJson(self, j) -> None:
    raise NotImplementedError()

  def SaveToJsonFile(self, file_path, **kwargs) -> None:
    with open(file_path, "w", encoding="utf-8") as f:
      json.dump(self.ToJson(), f, **kwargs)

  def LoadFromJsonFile(self, file_path) -> None:
    with open(file_path, "r", encoding="utf-8") as f:
      j = json.load(f)
      self.FromJson(j)

def AutoObjectToJsonHandler(obj):
  obj_class = type(obj)
  all_class_props = dir(obj_class)

  all_sub_props = dir(obj)
  name_value_dict = {}
  for prop_name in all_sub_props:
    if prop_name.startswith("__") and prop_name.endswith("__"):
      continue

    if prop_name in all_class_props:
      continue   # class member

    item = getattr(obj, prop_name)
    if isinstance(item, IJsonSerializable):
      item = item.ToJson()
    else:
      try: 
        json.dumps(item)
      except:
        continue

    name_value_dict[prop_name] = item
  return name_value_dict

def AutoObjectFromJsonHander(obj, j, allow_not_defined_attr=False):
  all_sub_props = dir(obj)
  for key, value in j.items():
    if not allow_not_defined_attr and not key in all_sub_props:
      # raise ValueError("cannot auto load json fields")
      continue
    setattr(obj, key, value)