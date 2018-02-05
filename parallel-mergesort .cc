//============================================================================
// Name        : parallel-mergesort.cc
// Author      : Hongjian Lu, Ruihong Wang
//============================================================================

#include <iostream>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <omp.h>
#include "sort.hh"
using namespace std;

// Binary Search
int BinarySearch(int x,keytype *A,int p,int r)
{
    int low=p,mid,high;
    high=max(p,r+1);
    while(low<high){
        mid=(low+high)/2;
        if(x<=A[mid]) high=mid;
        else low=mid+1;
    }
    return high;
}

//Sequential Merge(Thread=1)
void Merge(keytype *A,int p1,int r1,int p2,int r2,keytype* B,int p3)
{
    int length = r1-p1+1+r2-p2+1;
    for(;p1<=r1&&p2<=r2;B[p3++]=(A[p1]<A[p2]?A[p1++]:A[p2++]));
    for(;p1<=r1;B[p3++]=A[p1++]);
    for(;p2<=r2;B[p3++]=A[p2++]);
}

//Parallel Merge(Thread>1)
void PMerge(keytype* A,int p1,int r1,int p2,int r2,keytype* B,int p3,int threads)
{
    if(threads>1)
    {
        int q1,q2,q3;
        int n1 = r1-p1+1;
        int n2 = r2-p2+1;
        if (n1<n2){swap(n1,n2);swap(p1,p2);swap(r1,r2);}//Exchange to ensure n1>=n2
        if (n1==0)
            return;
        else
        {
            q1 = (p1+r1)/2;
            q2 = BinarySearch(A[q1],A,p2,r2); //A is int or not
            q3 = p3+(q1-p1)+(q2-p2);
            B[q3]=A[q1];
            #pragma omp parallel sections
            {
                #pragma omp section
                PMerge(A,p1,q1-1,p2,q2-1,B,p3,threads/2);
                #pragma omp section
                PMerge(A,q1+1,r1,q2,r2,B,q3+1,threads/2);
            }
        }
    }
    else Merge( A,p1,r1,p2,r2,B,p3);
}

//Sequential Mergesort(Thread=1)
void MSort(keytype* A,keytype* B,int L,int RightEnd,bool srcToDst=true)
{
    if (L == RightEnd) {    // termination/base case of sorting a single element
        if (srcToDst) B[L] = A[L];    // copy the single element from src to dst
        return;
    }
    int center;
    if(L<RightEnd)
    {
        center=(L+RightEnd)/2;
        MSort(A,B,L,center,!srcToDst);
        MSort(A,B,center+1,RightEnd,!srcToDst);
        if ( srcToDst )PMerge(A,L,center,center+1,RightEnd,B,L,1);
        else PMerge(B,L,center,center+1,RightEnd,A,L,1);
    }
}

//Parallel Mergesort(Thread>1)
void PMSort(keytype* A,keytype* B,int L,int RightEnd, int threads,bool srcToDst=true )
{
	if (L == RightEnd) {    // termination/base case of sorting a single element
        if (srcToDst)  B[L] = A[L];    // copy the single element from src to dst
        return;
	}
    if(threads>1){
        int center;
        if(L<RightEnd){
            center=(L+RightEnd)/2;
            #pragma omp parallel sections
            {
               #pragma omp section
               PMSort(A,B,L,center,threads/2,!srcToDst);
               #pragma omp section
               PMSort(A,B,center+1,RightEnd,threads/2,!srcToDst);
            }
            if (srcToDst)PMerge(A,L,center,center+1,RightEnd,B,L,threads);
            else PMerge(B,L,center,center+1,RightEnd,A,L,threads);
        }
    }
    else MSort(A,B,L,RightEnd,srcToDst);
}

//Mergesort
void Merge_sort(keytype* A,int N,int threads)
{
    keytype *Temp=(keytype *)malloc(N*sizeof(keytype));
    if(Temp)
    {                                                                   
       PMSort(A,Temp,0,N-1,threads);
       for(int i=0;i<N;i++) A[i]=Temp[i];
       free(Temp);
    }
    else printf("no space!\n");
}

void parallelSort (int N, keytype* A)
{
    //8 cores
    omp_set_nested(1);
    omp_set_num_threads(8);
    int threads =8;
    Merge_sort(A,N,threads);
}

