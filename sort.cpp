#include <stdio.h>

#include <tuple>

#include "types.h"
#include <thread>
#include <iostream>
#include <queue>
#include <math.h>

using namespace std;


/**
 * creating a tree struct  for lazy kmerger
 */
struct Node{
    size_t bufferSize;
    queue<KVPair> buffer;
    Node* left;
    Node* right;
};

size_t threadCap = 0;
vector<KVPair> ans;
/**
 * This is the fill function to fill the tree in lazy k-merge
 * @param head is the head node of the current recursion
 * @param array is the original, unsorted array
 * @param index is the current index in the array
 * @param n is the size/data count of the array, aka "n"
 */
void fill(Node* head, KVPair* array, int& index, int n){

    if(head->left == nullptr && head->right == nullptr) {

        if(index < n) {
            KVPair p;
            p.key = array[index].key;
            p.val = array[index].val;
            head->buffer.push(p);
//            cout << "[" << index << "]: " << array[index].key << endl;
            index++;
        }
        return;
    }

    while(head->buffer.size() < head->bufferSize){

        if(head->left != nullptr){
            fill(head->left, array, index, n);
        }
        if(head->right != nullptr){
            fill(head->right, array, index, n);
        }

        if(head->left != nullptr && head->right != nullptr){

            //merging
            if(!head->buffer.empty()){
                queue<KVPair> newQueue;

                while(newQueue.size() < head->bufferSize - head->buffer.size() && !head->buffer.empty()
                && !head->left->buffer.empty() && !head->right->buffer.empty()){
                        if (head->buffer.front().key <= head->left->buffer.front().key && head->buffer.front().key <=
                                                                                         head->right->buffer.front().key) {
                            newQueue.push(head->buffer.front());
                            head->buffer.pop();
                        } else if (head->left->buffer.front().key <= head->buffer.front().key &&
                                   head->left->buffer.front().key <= head->right->buffer.front().key) {
                            newQueue.push(head->left->buffer.front());
                            head->left->buffer.pop();
                        } else if (head->right->buffer.front().key <= head->buffer.front().key &&
                                   head->right->buffer.front().key <= head->left->buffer.front().key) {

                            newQueue.push(head->right->buffer.front());
                            head->right->buffer.pop();
                        }
                }
                while (newQueue.size() < head->bufferSize - head->buffer.size()
                       && !head->left->buffer.empty() && !head->buffer.empty()) {
                    if(head->buffer.front().key <= head->left->buffer.front().key){
                        newQueue.push(head->buffer.front());
                        head->buffer.pop();
                    }
                    else{

                        newQueue.push(head->left->buffer.front());
                        head->left->buffer.pop();
                    }
                }
                while (newQueue.size() < head->bufferSize - head->buffer.size()
                       && !head->right->buffer.empty() && !head->buffer.empty()) {
                    if(head->buffer.front().key <= head->right->buffer.front().key){

                        newQueue.push(head->buffer.front());
                        head->buffer.pop();
                    }
                    else{

                        newQueue.push(head->right->buffer.front());
                        head->right->buffer.pop();
                    }
                }
                while(newQueue.size() < head->bufferSize - head->buffer.size()
                      && !head->right->buffer.empty() && !head->left->buffer.empty()){
                    if(head->left->buffer.front().key <= head->right->buffer.front().key){

                        newQueue.push(head->left->buffer.front());
                        head->left->buffer.pop();
                    }
                    else{
                        newQueue.push(head->right->buffer.front());
                        head->right->buffer.pop();
                    }
                }
                while(!head->buffer.empty()){
                    newQueue.push(head->buffer.front());
                    head->buffer.pop();
                }
                head->buffer = newQueue;
            }
            while (head->buffer.size() < head->bufferSize && !head->left->buffer.empty() &&
                   !head->right->buffer.empty()) {
                if (head->left->buffer.front().key <= head->right->buffer.front().key) {

                    head->buffer.push(head->left->buffer.front());
                    head->left->buffer.pop();
                } else {

                    head->buffer.push(head->right->buffer.front());
                    head->right->buffer.pop();
                }
            }
            while (head->buffer.size() < head->bufferSize && !head->left->buffer.empty()) {
                head->buffer.push(head->left->buffer.front());
                head->left->buffer.pop();
            }
            while (head->buffer.size() < head->bufferSize && !head->right->buffer.empty()) {

                head->buffer.push(head->right->buffer.front());
                head->right->buffer.pop();
            }
        }
    }
}

/**
 * Spawning the tree with k buffers at the bottom
 * @param root is the root of the current recursion
 * @param currentLevel is the current level of the tree
 * @param levels are the number of levels to spawn log2(k)
 */
void spawnTree(Node* root, int currentLevel, int levels){
    if(currentLevel == levels){
        return;
    }
    currentLevel += 1;
    Node* left = new Node;
    left->bufferSize = (int) sqrt(root->bufferSize);

    Node* right = new Node;
    right->bufferSize = (int) sqrt(root->bufferSize);

    root->left = left;
    root->right = right;

    spawnTree(root->left, currentLevel, levels);
    spawnTree(root->right, currentLevel, levels);


}

/**
 * This is a lazy k-merger implementation for cache-efficient sorting
 * @param array is the original, unsorted array
 * @param data_cnt is the number of data inside the array
 * @param threads is the number of threads to be spawned
 */
void user_sort(KVPair* array, size_t data_cnt, size_t threads) {

    //K = N^(1/3)
    size_t n = data_cnt;
    size_t k = (size_t) round(pow(n, 1.0 / 3.0));

    //head node for the tree
    Node* head = new Node;
    head->bufferSize = n;

    int currentLevel = 0;

    //buffer tree levels
    int levels = (int) (log2(k) + 1);

    //spawn the full tree with buffers
    spawnTree(head, currentLevel, levels);

    //index for running through the array
    int index = 0;

    //start lazy k merge
    fill(head, array, index, n);

    //copy values into array
    for(int i = 0; i < n; i++){
        array[i] = head->buffer.front();
//        cout << "user sort " << "[" << i << "]: " << head->buffer.front().key << endl;

        head->buffer.pop();
    }

}