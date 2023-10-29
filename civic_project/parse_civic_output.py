
# with open('civic_output.txt','r') as f:
# 	txt = f.read()

# lines = txt.split('\n')

# error_lines = []
# for line in lines:
# 	if 'ERROR CIVIC API QUERY' in line:
# 		error_lines.append(int(line.split(" : ",1)[0]))


# for i in error_lines:
# 	grid['features'][i]['properties']['civic_query_error'] = True
