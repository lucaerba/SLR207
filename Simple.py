content = "Talent she for lively eat led sister. Entrance strongly packages she out rendered get quitting denoting led. Dwelling confined improved it he no doubtful raptures. Several carried through an of up attempt gravity. Situation to be at offending elsewhere distrusts if. Particular use for considered projection cultivated. Worth of do doubt shall it their. Extensive existence up me contained he pronounce do. Excellence inquietude assistance precaution any impression man sufficient. Call park out she wife face mean. Invitation excellence imprudence understood it continuing to. Ye show done an into. Fifteen winding related may hearted colonel are way studied. County suffer twenty or marked no moment in he. Meet shew or said like he. Valley silent cannot things so remain oh to elinor. Far merits season better tended any age hunted."
map = {}
#also remove punctuation
for word in content.split():
    if word in map:
        map[word] += 1
    else:
        map[word] = 1

#sort by increasing value and then alfabetically
sorted_map = sorted(map.items(), key=lambda x: (x[1],x[0]))
print(sorted_map)

#print total number of words
print(len(content.split()))