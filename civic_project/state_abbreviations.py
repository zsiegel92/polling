states = '''Alabama AL
Alaska  AK
Arizona AZ
Arkansas    AR
California  CA
Colorado    CO
Connecticut CT
Delaware    DE
Florida FL
Georgia GA
Hawaii  HI
Idaho   ID
Illinois    IL
Indiana IN
Iowa    IA
Kansas  KS
Kentucky    KY
Louisiana   LA
Maine   ME
Maryland    MD
Massachusetts   MA
Michigan    MI
Minnesota   MN
Mississippi MS
Missouri    MO
Montana MT
Nebraska    NE
Nevada  NV
New Hampshire   NH
New Jersey  NJ
New Mexico  NM
New York    NY
North Carolina  NC
North Dakota    ND
Ohio    OH
Oklahoma    OK
Oregon  OR
Pennsylvania    PA
Rhode Island    RI
South Carolina  SC
South Dakota    SD
Tennessee   TN
Texas   TX
Utah    UT
Vermont VT
Virginia    VA
Washington  WA
West Virginia   WV
Wisconsin   WI
Wyoming WY
Commonwealth/Territory: Abbreviation:
District of Columbia    DC
Marshall Islands    MH
Military "State":   Abbreviation:
Armed Forces Africa AE
Armed Forces Americas   AA
Armed Forces Canada AE
Armed Forces Europe AE
Armed Forces Middle East    AE
Armed Forces Pacific    AP'''
l = states.split("\n")
l = [s for s in l if "Abbreviation" not in s]
f = lambda s: s.rsplit(None,1)
l = list(map(f,l))

state_abbrevs = dict(((pair[1],pair[0]) for pair in l))
# [['Alabama', 'AL'], ['Alaska', 'AK'], ['Arizona', 'AZ'], ['Arkansas', 'AR'], ['California', 'CA'], ['Colorado', 'CO'], ['Connecticut', 'CT'], ['Delaware', 'DE'], ['Florida', 'FL'], ['Georgia', 'GA'], ['Hawaii', 'HI'], ['Idaho', 'ID'], ['Illinois', 'IL'], ['Indiana', 'IN'], ['Iowa', 'IA'], ['Kansas', 'KS'], ['Kentucky', 'KY'], ['Louisiana', 'LA'], ['Maine', 'ME'], ['Maryland', 'MD'], ['Massachusetts', 'MA'], ['Michigan', 'MI'], ['Minnesota', 'MN'], ['Mississippi', 'MS'], ['Missouri', 'MO'], ['Montana', 'MT'], ['Nebraska', 'NE'], ['Nevada', 'NV'], ['New Hampshire', 'NH'], ['New Jersey', 'NJ'], ['New Mexico', 'NM'], ['New York', 'NY'], ['North Carolina', 'NC'], ['North Dakota', 'ND'], ['Ohio', 'OH'], ['Oklahoma', 'OK'], ['Oregon', 'OR'], ['Pennsylvania', 'PA'], ['Rhode Island', 'RI'], ['South Carolina', 'SC'], ['South Dakota', 'SD'], ['Tennessee', 'TN'], ['Texas', 'TX'], ['Utah', 'UT'], ['Vermont', 'VT'], ['Virginia', 'VA'], ['Washington', 'WA'], ['West Virginia', 'WV'], ['Wisconsin', 'WI'], ['Wyoming', 'WY'], ['District of Columbia', 'DC'], ['Marshall Islands', 'MH'], ['Armed Forces Africa', 'AE'], ['Armed Forces Americas', 'AA'], ['Armed Forces Canada', 'AE'], ['Armed Forces Europe', 'AE'], ['Armed Forces Middle East', 'AE'], ['Armed Forces Pacific', 'AP']]
