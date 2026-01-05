package com.minidb.storage.index;

public class BPlusTreeDebug {
    public static void main(String[] args) {
        BPlusTree<Integer, String> tree = new BPlusTree<>(5);

        // 插入 100 个元素
        for (int i = 1; i <= 100; i++) {
            tree.insert(i, "Value_" + i);
        }

        System.out.println("Tree height: " + tree.getHeight());
        System.out.println("Tree size: " + tree.getSize());

        // 测试查找关键值
        System.out.println("\n=== Search Tests ===");
        int[] testKeys = {1, 25, 50, 75, 100};
        for (int key : testKeys) {
            String value = tree.search(key);
            System.out.println("search(" + key + ") = " + value + (value == null ? " ERROR!" : ""));
        }
    }
}
