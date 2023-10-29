s = '1Adams2Allen3Ashland4Ashtabula5Athens6Auglaize7Belmont8Brown9Butler10Carroll11Champaign12Clark13Clermont14Clinton15Columbiana16Coshocton17Crawford18Cuyahoga19Darke20Defiance21Delaware22Erie23Fairfield24Fayette25Franklin26Fulton27Gallia28Geauga29Greene30Guernsey31Hamilton32Hancock33Hardin34Harrison35Henry36Highland37Hocking38Holmes39Huron40Jackson41Jefferson42Knox43Lake44Lawrence45Licking46Logan47Lorain48Lucas49Madison50Mahoning51Marion52Medina53Meigs54Mercer55Miami56Monroe57Montgomery58Morgan59Morrow60Muskingum61Noble62Ottawa63Paulding64Perry65Pickaway66Pike67Portage68Preble69Putnam70Richland71Ross72Sandusky73Scioto74Seneca75Shelby76Stark77Summit78Trumbull79Tuscarawas80Union81Van Wert82Vinton83Warren84Washington85Wayne86Williams87Wood88Wyandot'


mapper = {}

for i in range(89):
	startOfInd = s.index(str(i))
	for j in range(startOfInd,len(s)):
		if s[j].isalpha():
			endOfInd = j-1
			break
	startOfWord = endOfInd + 1
	for j in range(startOfWord,len(s)):
		if s[j].isnumeric():
			endOfWord = j-1
			break
		if j == len(s)-1:
			endOfWord = j

	mapper[s[startOfWord:(endOfWord+1)]] = i

# print(mapper)
# print("{")
# for k in sorted(mapper.keys()):
# 	print(f"'{k}' : {mapper[k]},")
# print("}")



county_numbers = {
	'Adams' : 1,
	'Allen' : 2,
	'Ashland' : 3,
	'Ashtabula' : 4,
	'Athens' : 5,
	'Auglaize' : 6,
	'Belmont' : 7,
	'Brown' : 8,
	'Butler' : 9,
	'Carroll' : 10,
	'Champaign' : 11,
	'Clark' : 12,
	'Clermont' : 13,
	'Clinton' : 14,
	'Columbiana' : 15,
	'Coshocton' : 16,
	'Crawford' : 17,
	'Cuyahoga' : 18,
	'Darke' : 19,
	'Defiance' : 20,
	'Delaware' : 21,
	'Erie' : 22,
	'Fairfield' : 23,
	'Fayette' : 24,
	'Franklin' : 25,
	'Fulton' : 26,
	'Gallia' : 27,
	'Geauga' : 28,
	'Greene' : 29,
	'Guernsey' : 30,
	'Hamilton' : 31,
	'Hancock' : 32,
	'Hardin' : 33,
	'Harrison' : 34,
	'Henry' : 35,
	'Highland' : 36,
	'Hocking' : 37,
	'Holmes' : 38,
	'Huron' : 39,
	'Jackson' : 40,
	'Jefferson' : 41,
	'Knox' : 42,
	'Lake' : 43,
	'Lawrence' : 44,
	'Licking' : 45,
	'Logan' : 46,
	'Lorain' : 47,
	'Lucas' : 48,
	'Madison' : 49,
	'Mahoning' : 50,
	'Marion' : 51,
	'Medina' : 52,
	'Meigs' : 53,
	'Mercer' : 54,
	'Miami' : 55,
	'Monroe' : 56,
	'Montgomery' : 57,
	'Morgan' : 58,
	'Morrow' : 59,
	'Muskingum' : 60,
	'Noble' : 61,
	'Ottawa' : 62,
	'Paulding' : 63,
	'Perry' : 64,
	'Pickaway' : 65,
	'Pike' : 66,
	'Portage' : 67,
	'Preble' : 68,
	'Putnam' : 69,
	'Richland' : 70,
	'Ross' : 71,
	'Sandusky' : 72,
	'Scioto' : 73,
	'Seneca' : 74,
	'Shelby' : 75,
	'Stark' : 76,
	'Summit' : 77,
	'Trumbull' : 78,
	'Tuscarawas' : 79,
	'Union' : 80,
	'Van Wert' : 81,
	'Vinton' : 82,
	'Warren' : 83,
	'Washington' : 84,
	'Wayne' : 85,
	'Williams' : 86,
	'Wood' : 87,
	'Wyandot' : 88
}
# {'Carroll': 10, 'Adams': 1, 'Allen': 2, 'Ashland': 3, 'Ashtabula': 4, 'Athens': 5, 'Auglaize': 6, 'Belmont': 7, 'Brown': 8, 'Butler': 9, 'Champaign': 11, 'Clark': 12, 'Clermont': 13, 'Clinton': 14, 'Columbiana': 15, 'Coshocton': 16, 'Crawford': 17, 'Cuyahoga': 18, 'Darke': 19, 'Defiance': 20, 'Delaware': 21, 'Erie': 22, 'Fairfield': 23, 'Fayette': 24, 'Franklin': 25, 'Fulton': 26, 'Gallia': 27, 'Geauga': 28, 'Greene': 29, 'Guernsey': 30, 'Hamilton': 31, 'Hancock': 32, 'Hardin': 33, 'Harrison': 34, 'Henry': 35, 'Highland': 36, 'Hocking': 37, 'Holmes': 38, 'Huron': 39, 'Jackson': 40, 'Jefferson': 41, 'Knox': 42, 'Lake': 43, 'Lawrence': 44, 'Licking': 45, 'Logan': 46, 'Lorain': 47, 'Lucas': 48, 'Madison': 49, 'Mahoning': 50, 'Marion': 51, 'Medina': 52, 'Meigs': 53, 'Mercer': 54, 'Miami': 55, 'Monroe': 56, 'Montgomery': 57, 'Morgan': 58, 'Morrow': 59, 'Muskingum': 60, 'Noble': 61, 'Ottawa': 62, 'Paulding': 63, 'Perry': 64, 'Pickaway': 65, 'Pike': 66, 'Portage': 67, 'Preble': 68, 'Putnam': 69, 'Richland': 70, 'Ross': 71, 'Sandusky': 72, 'Scioto': 73, 'Seneca': 74, 'Shelby': 75, 'Stark': 76, 'Summit': 77, 'Trumbull': 78, 'Tuscarawas': 79, 'Union': 80, 'Van Wert': 81, 'Vinton': 82, 'Warren': 83, 'Washington': 84, 'Wayne': 85, 'Williams': 86, 'Wood': 87, 'Wyandot': 88}
