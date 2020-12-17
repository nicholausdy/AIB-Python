from concurrent.futures import ThreadPoolExecutor
import asyncio
import uvloop

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

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
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(executor, funcName, *params)
    return result
  except Exception as error:
    print(error)
    raise Exception(error)