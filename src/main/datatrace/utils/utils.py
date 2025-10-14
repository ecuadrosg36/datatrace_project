import re

def extract_regex_as_dict(pattern_dict, str_input) -> dict:
    """
    """
    pattern = re.compile(pattern_dict["pattern"], re.DOTALL | re.IGNORECASE | re.MULTILINE)
    match = pattern.search(str_input)
    if match:
        output_dict = match.groupdict()
        output_dict["flag_success"] = 1

        return output_dict
    else:
        output_dict = {e: None for e in pattern_dict["struct_list"]}
        output_dict["flag_success"] = 0

        return output_dict

def sub_patterns(sql_query, sub_list):
    """
    """
    if sql_query:
        for sub_dict in sub_list:
            pattern = sub_dict['pattern_to_sub']
            replacement = sub_dict['value_to_sub']
            sql_query = re.sub(pattern, replacement, sql_query, flags=re.IGNORECASE)

    return sql_query


def get_pattern_text(pattern, text, group_number=0):
    """
    """
    result = None
    if re.search(pattern, text, re.DOTALL | re.IGNORECASE | re.MULTILINE):
        result = re.search(pattern, text, re.DOTALL | re.IGNORECASE | re.MULTILINE).group(group_number)
    return result

def get_match_pattern_from_list(text, pattern_list):
    """
    """
    match_pattern_value = None
    if text:
        for pattern_temp in pattern_list:
            if match_pattern(pattern_temp, text):
                match_pattern_value = pattern_temp
                break
    return match_pattern_value

def match_pattern(pattern, text):
    """
    """
    if re.search(pattern, text, re.DOTALL | re.IGNORECASE | re.MULTILINE):
        return True
    else:
        return False