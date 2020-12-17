import asyncio
import json
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers

import os
from dotenv import load_dotenv

import predictor

BASE_DIR = os.getcwd()
env_path = BASE_DIR + '/app/.env'
load_dotenv(dotenv_path=env_path)

url = os.getenv('NATS_URL')

async def run_event_listener(loop):
  try:
    nc = NATS()
    await nc.connect(url, loop=loop)

    async def predictor_handler(msg):
      try:
        reply = msg.reply
        data = json.loads(msg.data.decode())
        result = await predictor.predict(data)
        await nc.publish(reply, json.dumps(result).encode())

      except (ErrConnectionClosed, ErrTimeout, ErrNoServers) as error:
        raise Exception(error)

    async def feature_importance_handler(msg):
      try:
        reply = msg.reply
        result = await predictor.feature_importance()
        await nc.publish(reply, json.dumps(result).encode())

      except (ErrConnectionClosed, ErrTimeout, ErrNoServers) as error:
        raise Exception(error)
    
    predictor_listener = await nc.subscribe('predictorCall','predictor.workers',predictor_handler)
    feature_importance_listener = await nc.subscribe('featureImportanceCall','feature.workers', feature_importance_handler)
  
  except Exception as error:
    print(error)

def run():
  loop = asyncio.get_event_loop()
  try:
    asyncio.ensure_future(run_event_listener(loop))
    loop.run_forever()

  except KeyboardInterrupt:
    pass

  finally:
    print('Closing loop')
    loop.close()

run()
    


