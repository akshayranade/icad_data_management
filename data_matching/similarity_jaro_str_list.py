"""
A function to get the closest string from a list of strings to the input string
"""
import textdistance
    
def similarity_jaro_str_list(s:str, str_list:list):
    match_list_jaro = []
    #Iterate over the provided list to find the jaro distance of each element to the given string
    for element in str_list:
        match_list_jaro.append(textdistance.jaro_winkler.similarity(s, element))
        
    #Get the index of element with highest similarity
    jaro_index = match_list_jaro.index(max(match_list_jaro))
    
    return (jaro_index, max(match_list_jaro))
    
    