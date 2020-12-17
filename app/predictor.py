from joblib import load
import asyncio

from util.async_util import async_transform
from util.file_util import json_loader

from pre_processor import prepare_data

import os

BASE_DIR = os.getcwd()

def load_model(directory):
  try:
    model = load(directory)
    return model
  except Exception as error:
    raise Exception(error)

rforest_regressor = load_model(BASE_DIR + '/app/files/tuned_random_forest_model.joblib')

# full pipeline from data preparation to prediction
async def predict(input_dict):
  try:
    input_df = await prepare_data(input_dict)
    result = await async_transform(rforest_regressor.predict, input_df.iloc[[0]])
    result_obj = {'success':True, 'message': {'adr': result[0]}}
    return result_obj
  
  except Exception as error:
    print(error)
    result_obj = {'success':False, 'message':'Prediction failed'}
    return result_obj

def load_feature_importance():
  try:
    obj = json_loader(BASE_DIR + '/app/files/top_adr_predictors.json')
    feature_list = []

    i = 0
    for key, value in obj.items():
      if (i == 10):
        break
      feature_list.append(key)
      i += 1
    
    return feature_list
  except Exception as error:
    print(error)
    raise Exception(error)

feature_imp_list = load_feature_importance()

async def feature_importance():
  try:
    result_obj = {'success': True, 'message': feature_imp_list}
    return result_obj

  except Exception as error:
    print(error)
    result_obj = {'success':False, 'message':'Fetch feature importance failed'}
    return result_obj

# async def main():
#   input_dict_full = {
#     'hotel':'Resort Hotel',
#     'is_canceled': 0,
#     'arrival_date_year': 2020,
#     'arrival_date_month':'July',
#     'arrival_date_week_number':27,
#     'arrival_date_day_of_month':1,
#     'meal':'FB',
#     'country':'Europe',
#     'market_segment':'Direct',
#     'distribution_channel':'Direct',
#     'is_repeated_guest':0,
#     'reserved_room_type':'A',
#     'assigned_room_type':'C',
#     'deposit_type':'No Deposit',
#     'agent':9,
#     'customer_type':'Transient',
#     'reservation_status':'Check-Out',
#     'reservation_status_date_year':2020,
#     'reservation_status_date_month':7,
#     'reservation_status_date_day':2,
#     'lead_time':7,
#     'stays_in_weekend_nights':0,
#     'stays_in_week_nights':1,
#     'adults':1, 
#     'children':0, 
#     'babies':0,
#     'previous_cancellations':0, 
#     'previous_bookings_not_canceled':0,
#     'days_in_waiting_list':0,
#     'required_car_parking_spaces':0,  
#     'total_of_special_requests':0,
#     'booking_changes':3
#   }

#   pred_result = await predict(input_dict_full)
#   print(pred_result)

#   feature_result = await feature_importance()
#   print(feature_result)

# if __name__ == "__main__":
#   loop = asyncio.get_event_loop()
#   loop.run_until_complete(main())
#   loop.close()

