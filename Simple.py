content = "dog cat car tree house bike cat dog car tree car cat dog bike tree house car cat dog tree bike house car dog cat bike tree house cat dog car tree bike house cat dog car tree bike house cat dog car tree bike house"
map = {}
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