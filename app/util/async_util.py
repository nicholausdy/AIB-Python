from concurrent.futures import ThreadPoolExecutor
from asyncio import get_event_loop

def threadExecutor():
  try:
    executor = ThreadPoolExecutor(max_workers=10)
    return executor
  except Exception as error:
    print(error)
    raise Exception('Failed getting thread executor')

executor = threadExecutor()

async def async_transform(funcName, *params):
  try:
    loop = get_event_loop()
    result = await loop.run_in_executor(executor, funcName, *params)
    return result
  except Exception as error:
    print(error)
    raise Exception(error)