"""
A function to get the closest string from a list of strings to the input string
"""
import textdistance
    
def similarity_cosine_str_list(s:str, str_list:list):
    match_list_cosine = []
    #Iterate over the provided list to find the jaro distance of each element to the given string
    for element in str_list:
        match_list_cosine.append(textdistance.cosine.similarity(s, element))
        
    #Get the index of element with highest similarity
    cosine_index = match_list_cosine.index(max(match_list_cosine))
    
    return (cosine_index, max(match_list_cosine))