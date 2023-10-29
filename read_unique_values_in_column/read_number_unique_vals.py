filename = "unique_vals.txt"
with open(filename,"r") as f:
	s = f.read()

ll = s.split("\n")
print(f"Number unique values: {len(set(ll))}")
