public class Simple {
    public static void main(String[] args) {
        String content = "dog cat car tree house bike cat dog car tree car cat dog bike tree house car cat dog tree bike house car dog cat bike tree house cat dog car tree bike house cat dog car tree bike house cat dog car tree bike house";
        HashMap<String, Integer> wordCount = new HashMap<String, Integer>();
        String[] words = content.split(" ");
        for (String word : words) {
            if (wordCount.containsKey(word)) {
                wordCount.put(word, wordCount.get(word) + 1);
            } else {
                wordCount.put(word, 1);
            }
        }
        System.out.println(wordCount);
        
    }
    
}
