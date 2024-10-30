"""
A function to get the standard school name from master_school table
w.r.t. the given string
"""
from data_matching.similarity_jaro_str_list import similarity_jaro_str_list

def get_standard_school(school_str, school_master_df):
    if school_str == '':
        return ('None', 'None', 'None', 0)
    
    #Read master schools table
    school_master = school_master_df
    
    #Combine names and addresses
    school_master['school_name_city'] = school_master['institute_name'] + ' ' + school_master['city']
    school_master['school_name_add1'] = school_master['institute_name'] + ' ' + school_master['address1']
    school_master['school_name_add2'] = school_master['institute_name'] + ' ' + school_master['address2']
    school_master['school_name_add1_city'] = school_master['institute_name'] + ' ' + school_master['address1'] + ' ' + school_master['city']
    school_master['school_name_add2_city'] = school_master['institute_name'] + ' ' + school_master['address2'] + ' ' + school_master['city']
    school_master['school_name_full_add'] = school_master['institute_name'] + ' ' + school_master['address1'] + ' ' + school_master['address2'] + ' ' + school_master['city'] 
    
    #Match a string with a school name
    standard_school_name = ''
    standard_school_addr = ''
    standard_school_id = 0
    

    #Check for exact match with either of the columns
    if school_str in school_master['school_name_city'].to_list():
        i = school_master['school_name_city'].index[school_master['school_name_city'].eq(school_str)][0]
    elif school_str in school_master['school_name_full_add'].to_list():
        i = school_master['school_name_full_add'].index[school_master['school_name_full_add'].eq(school_str)][0]
    elif school_str in school_master['school_name_add2_city'].to_list():
        i = school_master['school_name_add2_city'].index[school_master['school_name_add2_city'].eq(school_str)][0]
    elif school_str in school_master['school_name_add1_city'].to_list():
        i = school_master['school_name_add1_city'].index[school_master['school_name_add1_city'].eq(school_str)][0]
    elif school_str in school_master['school_name_add2'].to_list():
        i = school_master['school_name_add2'].index[school_master['school_name_add2'].eq(school_str)][0]
    elif school_str in school_master['school_name_add1'].to_list():
        i = school_master['school_name_add1'].index[school_master['school_name_add1'].eq(school_str)][0]
    elif school_str in school_master['institute_name'].to_list():
        i = school_master['institute_name'].index[school_master['institute_name'].eq(school_str)][0]
    #In case of no exact match, check for highest jaro similarity
    else:
        i = similarity_jaro_str_list(school_str, school_master['school_name_add1_city'].to_list())[0]
    
    standard_school_name = school_master.loc[i, 'institute_name']
    standard_school_addr = school_master.loc[i, 'address1']
    standard_school_city = school_master.loc[i, 'city']
    standard_school_id = school_master.loc[i, 'school_id']
        
    return (standard_school_name, standard_school_addr, standard_school_city, standard_school_id)
