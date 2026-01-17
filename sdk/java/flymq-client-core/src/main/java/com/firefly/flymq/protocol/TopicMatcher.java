package com.firefly.flymq.protocol;

public class TopicMatcher {
    
    public static boolean isPattern(String s) {
        return s.contains("+") || s.contains("#");
    }
    
    public static boolean matchPattern(String pattern, String topic) {
        String[] pLevels = pattern.split("/");
        String[] tLevels = topic.split("/");
        
        return matchLevels(pLevels, tLevels, 0, 0);
    }
    
    private static boolean matchLevels(String[] pLevels, String[] tLevels, int pIdx, int tIdx) {
        if (pIdx == pLevels.length && tIdx == tLevels.length) {
            return true;
        }
        if (pIdx == pLevels.length) {
            return false;
        }
        
        if (pLevels[pIdx].equals("#")) {
            return true;
        }
        
        if (tIdx == tLevels.length) {
            return false;
        }
        
        if (pLevels[pIdx].equals("+") || pLevels[pIdx].equals(tLevels[tIdx])) {
            return matchLevels(pLevels, tLevels, pIdx + 1, tIdx + 1);
        }
        
        return false;
    }
}
