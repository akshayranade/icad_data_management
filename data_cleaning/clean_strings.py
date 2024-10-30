import re
#A function to clean the string data including convert to upper case, remove non alpha numeric characters, remove multiple spaces
def clean_string_data(input_str:str) -> str:
    output_str = str(input_str)
    #Upper case
    output_str = output_str.upper()
    #Remove multiple spaces
    output_str = " ".join(output_str.split())
    output_str = re.sub(' +', ' ', output_str)
    #Remove non alphanumeric characters
    output_str = re.sub(r'[^A-Za-z0-9 ]+', '', output_str)
    
    return output_str