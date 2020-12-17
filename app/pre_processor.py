import pandas as pd
from pandas.api.types import CategoricalDtype
import asyncio
import uvloop

from util.file_util import json_loader
from util.async_util import async_transform

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

import os
BASE_DIR = os.getcwd()

def load_preliminary_json():
  try:
    mean_dict = json_loader(BASE_DIR + '/app/files/mean_cont.json')

    std_dict  = json_loader(BASE_DIR + '/app/files/std_cont.json')

    column_order = json_loader(BASE_DIR + '/app/files/column_order.json')

    return mean_dict, std_dict, column_order

  except Exception as error:
    raise Exception(error)

mean_dict, std_dict, column_order = load_preliminary_json()

def dict_to_dataframe(input_dict):
  try:
    out_dict = {}
    
    for key,value in input_dict.items():
      single_el_list = []
      single_el_list.append(value)
      out_dict[key] = single_el_list
    
    out_df = pd.DataFrame.from_dict(out_dict)
    return out_df
  
  except Exception as error:
    raise Exception(error)

async def year_mapper(input_dict, key):
  try:
    if (input_dict[key] < 2019):
      input_dict[key] = 2014
      return input_dict

    if (input_dict[key] > 2022):
      input_dict[key] = 2017
      return input_dict

    ref_dict = {
      2019 : 2014,
      2020 : 2015,
      2021 : 2016,
      2022 : 2017
    }
    key_1 = input_dict[key]
    input_dict[key] = ref_dict[key_1]

    return input_dict

  except Exception as error:
    raise Exception(error)



def normalize(input_dict):
  try:
    df = dict_to_dataframe(input_dict)
    for key, value in mean_dict.items():
      # calculate z_score
      df[key] = (df[key] - value) / std_dict[key]

    return df
  except Exception as error:
    raise Exception(error)

def bin_single_df(df, col_name, bins_range, bins_labels):
  try:
    new_col_name = col_name + '_binned'
    df[new_col_name] = pd.cut(df[col_name], bins_range, labels=bins_labels)
    df.drop(columns=[col_name], axis=1, inplace=True)

    return df
  except Exception as error:
    raise Exception(error)

async def bin_parallel(input_dict):
  try:
    bins_days_in_month = [0,7,14,21,28,32]
    bins_days_in_month_labels = ['week1','week2','week3','week4','week5']

    bins_weeks_in_year = [0,13,26,49,54]
    bins_week_in_year_labels = ['Q1','Q2','Q3','Q4']

    bins_agent = [0,100,200,300,400,500,600]
    bins_labels = ['1-100','101-200','201-300','301-400','401-500','501-600']

    df_result_list = await asyncio.gather(
      async_transform(dict_to_dataframe, {'arrival_date_day_of_month': input_dict['arrival_date_day_of_month']}),
      async_transform(dict_to_dataframe, {'reservation_status_date_day': input_dict['reservation_status_date_day']}),
      async_transform(dict_to_dataframe, {'arrival_date_week_number': input_dict['arrival_date_week_number']}),
      async_transform(dict_to_dataframe, {'agent': input_dict['agent']})
    )

    result_list = await asyncio.gather(
      async_transform(bin_single_df,df_result_list[0], 'arrival_date_day_of_month',bins_days_in_month, bins_days_in_month_labels),
      async_transform(bin_single_df,df_result_list[1], 'reservation_status_date_day',bins_days_in_month, bins_days_in_month_labels),
      async_transform(bin_single_df,df_result_list[2], 'arrival_date_week_number',bins_weeks_in_year, bins_week_in_year_labels),
      async_transform(bin_single_df, df_result_list[3], 'agent', bins_agent, bins_labels)
    )

    # result_df = pd.concat(result_list, axis=1)
    result_df = await async_transform(concat_df_on_column, result_list)
    return result_df

  except Exception as error:
    raise Exception(error) 

def one_hot_encode(input_dict, key, list_categories):
  try:
    df = dict_to_dataframe(input_dict)
    df[key] = df[key].astype(CategoricalDtype(list_categories))
    
    new_df = pd.get_dummies(df[key], prefix=key)

    return new_df
  except Exception as error:
    raise Exception(error)

def one_hot_encode_from_df(df, key, list_categories):
  try:
    df[key] = df[key].astype(CategoricalDtype(list_categories))
    
    new_df = pd.get_dummies(df[key], prefix=key)

    return new_df
  except Exception as error:
    raise Exception(error)

def concat_df_on_column(list_df):
  try:
    result_df = pd.concat(list_df, axis=1)
    return result_df
  except Exception as error:
    raise Exception(error)

def reindex_df(df,columns_list):
  try:
    result_df = df.reindex(columns=columns_list)
    return result_df
  except Exception as error:
    raise Exception(error)

async def convert_to_category_parallel(input_dict):
  try:
    # bin first
    binned_df = await bin_parallel({
      'arrival_date_day_of_month':input_dict['arrival_date_day_of_month'],
      'reservation_status_date_day':input_dict['reservation_status_date_day'],
      'arrival_date_week_number':input_dict['arrival_date_week_number'],
      'agent':input_dict['agent']
    })

    # declare categories
    hotel_cat = ['City Hotel','Resort Hotel']
    arrival_date_year_cat = [2015, 2016, 2017]
    arrival_date_month_cat = ['January','February','March','April','May','June','July','August','September','October','November','December']
    arrival_date_week_binned_cat = ['Q1','Q2','Q3','Q4']
    arrival_date_day_of_month_binned_cat = ['week1','week2','week3','week4','week5']

    meal_cat = ['BB','FB','HB','SC']
    country_cat = ['Africa','Antarctica','Asia','Caribbean','Central_America','Europe','North_America','Oceania','South_America']
    market_segment_cat = ['Aviation','Complementary','Corporate','Direct','Groups','Offline TA/TO','Online TA']
    distribution_channel_cat = ['Corporate','Direct','GDS','TA/TO']
    reserved_room_type_cat = ['A','B','C','D','E','F','G','H','L','P']

    assigned_room_type_cat = ['A','B','C','D','E','F','G','H','I','K','L','P']
    deposit_type_cat = ['No Deposit','Non Refund','Refundable']
    customer_type_cat = ['Contract','Group','Transient','Transient-Party']
    reservation_status_cat = ['Canceled','Check-Out','No-Show']
    reservation_status_date_year_cat = [2014,2015,2016,2017]
    
    reservation_status_date_month_cat = [1,2,3,4,5,6,7,8,9,10,11,12]
    reservation_status_date_day_binned_cat = ['week1','week2','week3','week4','week5']
    agent_binned_cat = ['1-100','101-200','201-300','301-400','401-500','501-600']

    # create dataframe from boolean columns
    bools = await asyncio.gather(
      async_transform(dict_to_dataframe, {'is_canceled': input_dict['is_canceled']}),
      async_transform(dict_to_dataframe, {'is_repeated_guest': input_dict['is_repeated_guest']})
    )

    #def one_hot_encode(input_dict, key, list_categories):
    #def one_hot_encode_from_df(df, key, list_categories)
    one_hot_result_list_1 = await asyncio.gather(
      async_transform(one_hot_encode, {'hotel':input_dict['hotel']},'hotel', hotel_cat),
      async_transform(one_hot_encode, {'arrival_date_year':input_dict['arrival_date_year']},'arrival_date_year',arrival_date_year_cat),
      async_transform(one_hot_encode, {'arrival_date_month':input_dict['arrival_date_month']},'arrival_date_month',arrival_date_month_cat),
      async_transform(one_hot_encode_from_df, binned_df,'arrival_date_week_number_binned',arrival_date_week_binned_cat),
      async_transform(one_hot_encode_from_df, binned_df,'arrival_date_day_of_month_binned',arrival_date_day_of_month_binned_cat),
    )

    one_hot_result_list_2 = await asyncio.gather(
      async_transform(one_hot_encode, {'meal':input_dict['meal']},'meal', meal_cat),
      async_transform(one_hot_encode, {'country':input_dict['country']},'country',country_cat),
      async_transform(one_hot_encode, {'market_segment':input_dict['market_segment']},'market_segment',market_segment_cat),
      async_transform(one_hot_encode, {'distribution_channel':input_dict['distribution_channel']},'distribution_channel',distribution_channel_cat),
      async_transform(one_hot_encode, {'reserved_room_type':input_dict['reserved_room_type']},'reserved_room_type', reserved_room_type_cat)
    )

    one_hot_result_list_3 = await asyncio.gather(
      async_transform(one_hot_encode, {'assigned_room_type':input_dict['assigned_room_type']},'assigned_room_type', assigned_room_type_cat),
      async_transform(one_hot_encode, {'deposit_type':input_dict['deposit_type']},'deposit_type', deposit_type_cat),
      async_transform(one_hot_encode, {'customer_type':input_dict['customer_type']},'customer_type', customer_type_cat),
      async_transform(one_hot_encode, {'reservation_status':input_dict['reservation_status']},'reservation_status', reservation_status_cat),
      async_transform(one_hot_encode, {'reservation_status_date_year':input_dict['reservation_status_date_year']},'reservation_status_date_year', reservation_status_date_year_cat)
    )

    one_hot_result_list_4 = await asyncio.gather(
      async_transform(one_hot_encode, {'reservation_status_date_month':input_dict['reservation_status_date_month']},'reservation_status_date_month', reservation_status_date_month_cat),
      async_transform(one_hot_encode_from_df, binned_df,'reservation_status_date_day_binned', reservation_status_date_day_binned_cat),
      async_transform(one_hot_encode_from_df, binned_df,'agent_binned', agent_binned_cat),
    )

    # combine multiple lists of dataframe to single list of dataframe
    result_list = bools + one_hot_result_list_1 + one_hot_result_list_2 + one_hot_result_list_3 + one_hot_result_list_4

    # result_df = pd.concat(result_list, axis=1)
    result_df = await async_transform(concat_df_on_column, result_list)
    return result_df

  except Exception as error:
    raise Exception(error)

async def prepare_data(input_dict):
  try:
    input_dict = await year_mapper(input_dict, 'arrival_date_year')
    input_dict = await year_mapper(input_dict, 'reservation_status_date_year')

    input_dict_cont = { 
      'lead_time':input_dict['lead_time'],
      'stays_in_weekend_nights': input_dict['stays_in_weekend_nights'],
      'stays_in_week_nights': input_dict['stays_in_week_nights'],
      'adults': input_dict['adults'],
      'children': input_dict['children'], 
      'babies':input_dict['babies'],
      'previous_cancellations': input_dict['previous_cancellations'], 
      'previous_bookings_not_canceled': input_dict['previous_bookings_not_canceled'],
      'days_in_waiting_list': input_dict['days_in_waiting_list'],
      'required_car_parking_spaces': input_dict['required_car_parking_spaces'],  
      'total_of_special_requests': input_dict['total_of_special_requests'],
      'booking_changes': input_dict['booking_changes']
    }

    input_dict_cat = {
      'hotel':input_dict['hotel'],
      'is_canceled': input_dict['is_canceled'],
      'arrival_date_year': input_dict['arrival_date_year'],
      'arrival_date_month': input_dict['arrival_date_month'],
      'arrival_date_week_number': input_dict['arrival_date_week_number'],
      'arrival_date_day_of_month': input_dict['arrival_date_day_of_month'],
      'meal': input_dict['meal'],
      'country':input_dict['country'],
      'market_segment':input_dict['market_segment'],
      'distribution_channel':input_dict['distribution_channel'],
      'is_repeated_guest': input_dict['is_repeated_guest'],
      'reserved_room_type': input_dict['reserved_room_type'],
      'assigned_room_type': input_dict['assigned_room_type'],
      'deposit_type':input_dict['deposit_type'],
      'agent': input_dict['agent'],
      'customer_type': input_dict['customer_type'],
      'reservation_status':input_dict['reservation_status'],
      'reservation_status_date_year': input_dict['reservation_status_date_year'],
      'reservation_status_date_month': input_dict['reservation_status_date_month'],
      'reservation_status_date_day': input_dict['reservation_status_date_day']
    }

    # preprocess continuous and categorical variables in parallel
    result_list = await asyncio.gather(
      async_transform(normalize, input_dict_cont),
      convert_to_category_parallel(input_dict_cat)
    )

    # result_df = pd.concat(result_list, axis=1)
    result_df = await async_transform(concat_df_on_column, result_list)
    # ordered_df = result_df.reindex(columns=column_order['order'])
    ordered_df = await async_transform(reindex_df, result_df, column_order['order'])
    
    return ordered_df

  except Exception as error:
    raise Exception(error)


# tester
# async def main():
#   input_dict = { 'lead_time':342,'stays_in_weekend_nights':0,
#   'stays_in_week_nights':0,'adults':2, 'children':0, 'babies':0,'previous_cancellations':0, 
#   'previous_bookings_not_canceled':0,'days_in_waiting_list':0,
#   'required_car_parking_spaces':0,  'total_of_special_requests':0,
#   'booking_changes':3}

#   norm = normalize(input_dict)
#   print(norm)

#   input_dict_2 = {'arrival_date_day_of_month':1}
#   df = dict_to_dataframe(input_dict_2)
#   df2 = bin_single_df(df, 'arrival_date_day_of_month',[0,7,14,21,28,32],['week1','week2','week3','week4','week5'])
#   print(df2)

#   input_dict_3 = {'arrival_date_day_of_month':1,'reservation_status_date_day':1, 
#   'arrival_date_week_number':27, 'agent': 9}
#   df3 = await bin_parallel(input_dict_3)
#   print(df3)

#   input_dict_4 = {'hotel':'Resort Hotel'}
#   df4 = one_hot_encode(input_dict_4,'hotel',['Resort Hotel','City Hotel'])
#   print(df4)

#   df5 = one_hot_encode_from_df(df3, 'agent_binned',['1-100','101-200','201-300','301-400','401-500','501-600'])
#   print(df5)

#   input_dict_categorical = {
#     'hotel':'Resort Hotel',
#     'is_canceled': 0,
#     'arrival_date_year': 2015,
#     'arrival_date_month':'July',
#     'arrival_date_week_number':27,
#     'arrival_date_day_of_month':1,
#     'meal':'BB',
#     'country':'Europe',
#     'market_segment':'Direct',
#     'distribution_channel':'Direct',
#     'is_repeated_guest':0,
#     'reserved_room_type':'C',
#     'assigned_room_type':'C',
#     'deposit_type':'No Deposit',
#     'agent':9,
#     'customer_type':'Transient',
#     'reservation_status':'Check-Out',
#     'reservation_status_date_year':2015,
#     'reservation_status_date_month':7,
#     'reservation_status_date_day':1
#   }

#   df6 = await convert_to_category_parallel(input_dict_categorical)
#   print(df6)

#   input_dict_full = {
#     'hotel':'Resort Hotel',
#     'is_canceled': 0,
#     'arrival_date_year': 2015,
#     'arrival_date_month':'July',
#     'arrival_date_week_number':27,
#     'arrival_date_day_of_month':1,
#     'meal':'BB',
#     'country':'Europe',
#     'market_segment':'Direct',
#     'distribution_channel':'Direct',
#     'is_repeated_guest':0,
#     'reserved_room_type':'C',
#     'assigned_room_type':'C',
#     'deposit_type':'No Deposit',
#     'agent':9,
#     'customer_type':'Transient',
#     'reservation_status':'Check-Out',
#     'reservation_status_date_year':2015,
#     'reservation_status_date_month':7,
#     'reservation_status_date_day':1,
#     'lead_time':342,
#     'stays_in_weekend_nights':0,
#     'stays_in_week_nights':0,
#     'adults':2, 
#     'children':0, 
#     'babies':0,
#     'previous_cancellations':0, 
#     'previous_bookings_not_canceled':0,
#     'days_in_waiting_list':0,
#     'required_car_parking_spaces':0,  
#     'total_of_special_requests':0,
#     'booking_changes':3
#   }

#   df7 = await prepare_data(input_dict_full)
#   print(df7)
#   print(df7['agent_binned_1-100'])
  

# if __name__ == "__main__":
#   loop = asyncio.get_event_loop()
#   loop.run_until_complete(main())
#   loop.close()